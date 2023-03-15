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

package top

import (
	"bytes"
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
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []topTestCase
)

func init() {
	tcs = []topTestCase{
		newTestCase(mpool.MustNewZero(), []types.Type{{Oid: types.T_int8}}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(mpool.MustNewZero(), []types.Type{{Oid: types.T_int8}}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase(mpool.MustNewZero(), []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
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
		{
			bat := newBatch(t, tc.types, tc.proc, Rows)
			tc.proc.SetInputBatch(bat)
			_, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			bat.Clean(tc.proc.Mp())
		}
		{
			bat := newBatch(t, tc.types, tc.proc, Rows)
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

func newTestCase(m *mpool.MPool, ts []types.Type, limit int64, fs []*plan.OrderBySpec) topTestCase {
	return topTestCase{
		types: ts,
		proc:  testutil.NewProcessWithMPool(m),
		arg: &Argument{
			Fs:    fs,
			Types: ts,
			Limit: limit,
		},
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

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
