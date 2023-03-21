// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package semi

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.Argument
}

var (
	tcs []joinTestCase
)

func init() {
	tcs = []joinTestCase{
		newTestCase(mpool.MustNewZero(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []int32{0},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
		newTestCase(mpool.MustNewZero(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []int32{0},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestJoin(t *testing.T) {
	for _, tc := range tcs {
		hashBat := hashBuild(t, tc)
		if jm, ok := hashBat.Ht.(*hashmap.JoinMap); ok {
			jm.SetDupCount(int64(1))
		}
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)

		inputBatch := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		inputBatch.AddCnt(4)
		tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- hashBat
		for {
			if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
				break
			}
		}
		require.Equal(t, int64(1), inputBatch.Cnt)
		inputBatch.Clean(tc.proc.Mp())
		tc.arg.Free(tc.proc, false)
		hashBat.Clean(tc.proc.Mp())
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkJoin(b *testing.B) {
	testType := types.Type{Oid: types.T_int8}
	for i := 0; i < b.N; i++ {
		tcs = []joinTestCase{
			newTestCase(mpool.MustNewZero(), []bool{false}, []types.Type{testType}, []int32{0},
				[][]*plan.Expr{
					{
						newExpr(0, testType),
					},
					{
						newExpr(0, testType),
					},
				}),
			newTestCase(mpool.MustNewZero(), []bool{true}, []types.Type{testType}, []int32{0},
				[][]*plan.Expr{
					{
						newExpr(0, testType),
					},
					{
						newExpr(0, testType),
					},
				}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			hashBatch := hashBuild(t, tc)
			if jm, ok := hashBatch.Ht.(*hashmap.JoinMap); ok {
				jm.SetDupCount(int64(1))
			}
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			inputBatch := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			inputBatch.AddCnt(4)
			tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- hashBatch
			for {
				if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
					break
				}
			}
			inputBatch.Clean(tc.proc.Mp())
			hashBatch.Clean(tc.proc.Mp())
			tc.arg.Free(tc.proc, false)
		}
	}
}

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: &plan.Type{
			Size:  typ.Size,
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    int32(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(m *mpool.MPool, flgs []bool, ts []types.Type, rp []int32, cs [][]*plan.Expr) joinTestCase {
	proc := testutil.NewProcessWithMPool(m)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	fid := function.EncodeOverloadID(function.EQUAL, 4)
	args := make([]*plan.Expr, 0, 2)
	args = append(args, &plan.Expr{
		Typ: &plan.Type{
			Size: ts[0].Size,
			Id:   int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
	})
	args = append(args, &plan.Expr{
		Typ: &plan.Type{
			Size: ts[0].Size,
			Id:   int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 0,
			},
		},
	})
	cond := &plan.Expr{
		Typ: &plan.Type{
			Size: 1,
			Id:   int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: args,
				Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
			},
		},
	}
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Types:      ts,
			Result:     rp,
			Conditions: cs,
			Cond:       cond,
		},
		barg: &hashbuild.Argument{
			Types:       ts,
			NeedHashMap: true,
			Conditions:  cs[1],
		},
	}
}

func hashBuild(t *testing.T, tc joinTestCase) *batch.Batch {
	err := hashbuild.Prepare(tc.proc, tc.barg)
	require.NoError(t, err)
	inputBat := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
	defer inputBat.Clean(tc.proc.GetMPool())
	tc.proc.Reg.MergeReceivers[0].Ch <- inputBat
	tc.proc.Reg.MergeReceivers[0].Ch <- nil
	ok, err := hashbuild.Call(0, tc.proc, tc.barg, false, false)
	require.NoError(t, err)
	require.Equal(t, true, ok)
	return tc.proc.Reg.InputBatch
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
