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

package offset

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
type offsetTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []offsetTestCase
)

func init() {
	testTypes := []types.Type{{Oid: types.T_int8}}
	tcs = []offsetTestCase{
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Offset: 0,
				Types:  testTypes,
			},
		},
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Offset: 8,
				Types:  testTypes,
			},
		},
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Offset: 10,
				Types:  testTypes,
			},
		},
		{
			proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: testTypes,
			arg: &Argument{
				Offset: 12,
				Types:  testTypes,
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

func TestOffset(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		inputBatch := newBatch(t, tc.types, tc.proc, Rows)
		currentInputRow := 0
		end := false
		{ // test normal input batch
			tc.proc.SetInputBatch(inputBatch)
			currentInputRow += Rows
			_, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			if currentInputRow > int(tc.arg.Offset) {
				outLen := tc.proc.Reg.InputBatch.Length()
				expectLen := currentInputRow - int(tc.arg.Offset)
				require.Equal(t, expectLen, outLen)
			} else {
				require.Equal(t, 0, tc.proc.Reg.InputBatch.Length())
			}
		}

		{ // test empty input batch
			tc.proc.SetInputBatch(&batch.Batch{})
			end, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			require.Equal(t, false, end)
		}

		{ // test nil input batch
			tc.proc.SetInputBatch(nil)
			end, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			require.Equal(t, true, end)
		}
		tc.arg.Free(tc.proc, false)
		inputBatch.Clean(tc.proc.GetMPool())
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
