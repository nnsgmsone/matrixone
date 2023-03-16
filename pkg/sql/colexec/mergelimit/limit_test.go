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

package mergelimit

import (
	"bytes"
	"context"
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
)

// add unit tests for cases
type limitTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []limitTestCase
)

func init() {
	testTypes := []types.Type{{Oid: types.T_int8}}
	tcs = []limitTestCase{
		newTestCase(0, testTypes),
		newTestCase(18, testTypes),
		newTestCase(20, testTypes),
		newTestCase(22, testTypes),
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

func TestLimit(t *testing.T) {
	for _, tc := range tcs {
		inputBatch := newBatch(t, tc.types, tc.proc, Rows)
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		// put the batch input executor
		tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[0].Ch <- inputBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil

		ok, err := Call(0, tc.proc, tc.arg, false, false)
		require.NoError(t, err)
		output := tc.proc.Reg.InputBatch
		if output != nil {
			require.Equal(t, false, ok)
			outputLen := output.Length()
			expectLen := minValue(outputLen, int(tc.arg.Limit))
			require.Equal(t, outputLen, expectLen)
			require.Equal(t, inputBatch.Zs[:expectLen], output.Zs[:expectLen])
		} else {
			require.Equal(t, true, ok)
		}

		inputBatch.Clean(tc.proc.GetMPool())
		tc.arg.Free(tc.proc, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkLimit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testTypes := []types.Type{{Oid: types.T_int8}}
		tcs = []limitTestCase{
			newTestCase(18, testTypes),
			newTestCase(20, testTypes),
			newTestCase(22, testTypes),
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			for {
				if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
					if tc.proc.Reg.InputBatch != nil {
						tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
					}
					break
				}
				if tc.proc.Reg.InputBatch != nil {
					tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
				}
			}
			for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
				for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
					bat := <-tc.proc.Reg.MergeReceivers[i].Ch
					if bat != nil {
						bat.Clean(tc.proc.Mp())
					}
				}
			}
		}
	}
}

func newTestCase(limit uint64, testTypes []types.Type) limitTestCase {
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	return limitTestCase{
		proc:  proc,
		types: testTypes,
		arg: &Argument{
			Limit: limit,
			Types: testTypes,
		},
		cancel: cancel,
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func minValue(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
