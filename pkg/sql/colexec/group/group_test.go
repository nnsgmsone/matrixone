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

package group

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"

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
type groupTestCase struct {
	arg   *Argument
	flgs  []bool // flgs[i] == true: nullable
	types []types.Type
	proc  *process.Process
}

var (
	tcs []groupTestCase
)

func init() {
	tcs = []groupTestCase{
		newTestCase([]bool{false}, []types.Type{{Oid: types.T_int8}}, []*plan.Expr{},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
		newTestCase([]bool{false}, []types.Type{{Oid: types.T_int8}}, []*plan.Expr{newExpression(0, int32(types.T_int8))},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int8},
			{Oid: types.T_int16},
		}, []*plan.Expr{newExpression(0, int32(types.T_int8)), newExpression(1, int32(types.T_int16))},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int8},
			{Oid: types.T_int16},
			{Oid: types.T_int32},
			{Oid: types.T_int64},
		}, []*plan.Expr{newExpression(0, int32(types.T_int8)), newExpression(3, int32(types.T_int64))},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int64},
			{Oid: types.T_int64},
			{Oid: types.T_int64},
			{Oid: types.T_decimal128},
		}, []*plan.Expr{newExpression(1, int32(types.T_int64)), newExpression(3, int32(types.T_decimal128))},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int64},
			{Oid: types.T_int64},
			{Oid: types.T_int64},
			{Oid: types.T_decimal128},
		}, []*plan.Expr{newExpression(1, int32(types.T_int64)), newExpression(2, int32(types.T_int64)), newExpression(3, int32(types.T_decimal128))},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int64},
			{Oid: types.T_int64},
			{Oid: types.T_varchar},
			{Oid: types.T_decimal128},
		}, []*plan.Expr{newExpression(1, int32(types.T_int64)), newExpression(2, int32(types.T_varchar)), newExpression(3, int32(types.T_decimal128))},
			[]agg.Aggregate{{Op: 0, E: newExpression(0, int32(types.T_int64))}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestGroup(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		{
			bat := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.SetInputBatch(bat)
			_, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			bat.Clean(tc.proc.Mp())
		}
		{
			bat := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.SetInputBatch(bat)
			_, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			bat.Clean(tc.proc.Mp())
		}
		{
			tc.proc.SetInputBatch(&batch.Batch{})
			_, _ = Call(0, tc.proc, tc.arg, false, false)
		}
		{
			tc.proc.SetInputBatch(nil)
			_, _ = Call(0, tc.proc, tc.arg, false, false)
		}
		tc.arg.Free(tc.proc, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func newTestCase(flgs []bool, ts []types.Type, exprs []*plan.Expr, aggs []agg.Aggregate) groupTestCase {
	typs := make([]types.Type, len(exprs))
	for i, expr := range exprs {
		t := expr.Typ
		typs[i] = types.Type{
			Oid:   types.T(t.Id),
			Width: t.Width,
			Size:  t.Size,
			Scale: t.Scale,
		}
	}
	return groupTestCase{
		types: ts,
		flgs:  flgs,
		proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
		arg: &Argument{
			Types: typs,
			Aggs:  aggs,
			Exprs: exprs,
		},
	}
}

func newExpression(pos int32, oid int32) *plan.Expr {
	typ := new(plan.Type)
	typ.Id = oid
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
