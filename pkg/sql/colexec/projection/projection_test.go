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

package projection

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
type projectionTestCase struct {
	arg         *Argument
	inputTypes  []types.Type
	resultTypes []types.Type
	proc        *process.Process
}

var (
	tcs []projectionTestCase
)

func init() {
	tcs = []projectionTestCase{
		{
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			inputTypes: []types.Type{
				{Oid: types.T_int8},
				{Oid: types.T_int16},
				{Oid: types.T_int32},
			},
			resultTypes: []types.Type{
				{Oid: types.T_int32},
			},
			arg: &Argument{
				Es: []*plan.Expr{
					{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2}}}, // get col[2] from input batch
				},
				Types: []types.Type{
					{Oid: types.T_int32},
				}, // should same with resultTypes
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestProjection(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		inputBatch := newBatch(t, tc.inputTypes, tc.proc, Rows)
		expeactBatch := newBatch(t, tc.resultTypes, tc.proc, Rows)

		for cnt := 0; cnt < 2; cnt++ {
			tc.proc.Reg.InputBatch = inputBatch
			_, _ = Call(0, tc.proc, tc.arg, false, false)
			resultBat := tc.proc.Reg.InputBatch // compare output batch with resultBat
			for i, v := range expeactBatch.Vecs {
				// TODO: could add data check
				require.Equal(t, v.GetType(), resultBat.Vecs[i].GetType())
			}
			require.Equal(t, resultBat.Zs, expeactBatch.Zs)
		}

		// test empty batch
		tc.proc.Reg.InputBatch = &batch.Batch{}
		end, err := Call(0, tc.proc, tc.arg, false, false)
		require.Equal(t, false, end)
		require.NoError(t, err)

		// test nil batch
		tc.proc.Reg.InputBatch = nil
		end, err = Call(0, tc.proc, tc.arg, false, false)
		require.Equal(t, true, end)
		require.NoError(t, err)

		// test mem
		inputBatch.Clean(tc.proc.Mp())
		expeactBatch.Clean(tc.proc.Mp())
		tc.arg.Free(tc.proc, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
