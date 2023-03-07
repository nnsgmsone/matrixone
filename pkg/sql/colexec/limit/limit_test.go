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

package limit

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type limitTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []limitTestCase
)

func init() {
	testTypes := []types.Type{{Oid: types.T_int8}}
	tcs = []limitTestCase{
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Limit: 8,
				Types: testTypes,
			},
		},
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Limit: 10,
				Types: testTypes,
			},
		},
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Limit: 12,
				Types: testTypes,
			},
		},
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Limit: 0,
				Types: testTypes,
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

func TestLimit(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)

		// test input normal batch
		inputBatch := newBatch(t, tc.types, tc.proc, Rows)
		end := false
		tc.proc.SetInputBatch(inputBatch)
		end, err = Call(0, tc.proc, tc.arg, false, false)
		require.NoError(t, err)
		outputBatch := tc.proc.Reg.InputBatch
		if outputBatch != nil {
			outputLen := outputBatch.Length()
			expectLen := minValue(outputLen, int(tc.arg.Limit))
			require.Equal(t, expectLen, outputLen)
			require.Equal(t, inputBatch.Zs[:expectLen], outputBatch.Zs[:expectLen])
		} else { // corner case: limit == 0
			require.Equal(t, 0, int(tc.arg.Limit))
			require.Equal(t, true, end)
		}

		{ // test input empty batch
			newEnd := false
			tc.proc.SetInputBatch(&batch.Batch{})
			newEnd, err = Call(0, tc.proc, tc.arg, false, false)
			require.Equal(t, end, newEnd)
			require.NoError(t, err)
		}

		{ // test input nil batch
			tc.proc.SetInputBatch(nil)
			end, err = Call(0, tc.proc, tc.arg, false, false)
			require.Equal(t, true, end)
			require.NoError(t, err)
		}

		// test mem
		inputBatch.Clean(tc.proc.Mp())
		tc.arg.Free(tc.proc, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func minValue(a, b int) int {
	if a > b {
		return b
	}
	return a
}
