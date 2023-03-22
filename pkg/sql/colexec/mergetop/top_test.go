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

package mergetop

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type topTestCase struct {
	ds     []bool // Directions, ds[i] == true: the attrs[i] are in descending order
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []topTestCase
)

func init() {
	tcs = []topTestCase{
		newTestCase([]bool{false}, []types.Type{{Oid: types.T_int8, Size: 1}}, 3,
			[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]bool{true}, []types.Type{{Oid: types.T_int8, Size: 1}}, 3,
			[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]bool{false, false}, []types.Type{{Oid: types.T_int8, Size: 1}, {Oid: types.T_int64, Size: 8}},
			3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]bool{true, false}, []types.Type{{Oid: types.T_int8, Size: 1}, {Oid: types.T_int64, Size: 8}},
			3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]bool{true, false}, []types.Type{{Oid: types.T_int8, Size: 1}, {Oid: types.T_int64, Size: 8}},
			3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestTop(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			for {
				if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
					wg.Done()
					break
				}
			}
		}()
		bat0 := newBatch(t, tc.ds, tc.types, tc.proc, Rows)
		bat0.AddCnt(1)
		tc.proc.Reg.MergeReceivers[0].Ch <- bat0
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		bat1 := newBatch(t, tc.ds, tc.types, tc.proc, Rows)
		bat1.AddCnt(1)
		tc.proc.Reg.MergeReceivers[0].Ch <- bat1
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		wg.Wait()
		bat0.Clean(tc.proc.Mp())
		bat1.Clean(tc.proc.Mp())
		tc.arg.Free(tc.proc, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func newTestCase(ds []bool, ts []types.Type, limit int64, fs []*plan.OrderBySpec) topTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc := process.NewFromProc(testutil.NewProcessWithMPool(mpool.MustNewZero()),
		ctx, 2)
	return topTestCase{
		ds:    ds,
		types: ts,
		proc:  proc,
		arg: &Argument{
			Fs:             fs,
			Types:          ts,
			Limit:          limit,
			ChildrenNumber: 2,
		},
		cancel: cancel,
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

// create a new block based on the type information, ds[i] == true: in descending order
func newBatch(t *testing.T, ds []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
