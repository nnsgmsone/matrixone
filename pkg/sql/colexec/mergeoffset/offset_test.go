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

package mergeoffset

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10      // default rows
	BenchmarkRows = 1000000 // default rows for benchmark
	regNum        = 2       // test reg number
)

// add unit tests for cases
type offsetTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []offsetTestCase
)

func init() {
	tcs = []offsetTestCase{
		newTestCase(0),
		newTestCase(18),
		newTestCase(20),
		newTestCase(22),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
	}
}

func TestOffset(t *testing.T) {
	for _, tc := range tcs {
		inputBatch := newBatch(t, tc.types, tc.proc, Rows)
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		for i := 0; i < regNum; i++ {
			tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
		}

		outputRowCnt := 0
		for {
			end, err := Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			if tc.proc.Reg.InputBatch != nil {
				outputRowCnt += tc.proc.Reg.InputBatch.Length()
			}
			if end {
				fmt.Printf("Rows*regNum = %d, offset = %d", Rows*regNum, tc.arg.Offset)
				if Rows*regNum > tc.arg.Offset {
					expectLen := Rows*regNum - int(tc.arg.Offset)
					require.Equal(t, expectLen, outputRowCnt)
				}
				break
			}
		}
		tc.arg.Free(tc.proc, false)
		inputBatch.Clean(tc.proc.GetMPool())
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkOffset(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []offsetTestCase{
			newTestCase(8),
			newTestCase(10),
			newTestCase(12),
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			inputBatch := newBatch(t, tc.types, tc.proc, Rows)
			for i := 0; i < regNum; i++ {
				tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
				tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
				tc.proc.Reg.MergeReceivers[0].Ch <- nil
			}
			for {
				if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
					break
				}
			}
			tc.arg.Free(tc.proc, false)
			inputBatch.Clean(tc.proc.GetMPool())
			require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
		}
	}
}

func newTestCase(offset uint64) offsetTestCase {
	testTypes := []types.Type{{Oid: types.T_int8}}
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, regNum*3+1),
	}

	return offsetTestCase{
		proc:  proc,
		types: testTypes,
		arg: &Argument{
			Offset:         offset,
			Types:          testTypes,
			ChildrenNumber: regNum,
		},
		cancel: cancel,
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
