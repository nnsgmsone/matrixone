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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows   = 10 // default rows
	regNum = 2
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
	testTypes := []types.Type{{Oid: types.T_int8, Size: 1}}
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

func TestLimit(t *testing.T) {
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
			tc.proc.Cancel()
			tc.arg.Free(tc.proc, false)
		}()
		bat0 := newBatch(t, tc.types, tc.proc, Rows)
		select {
		case <-tc.proc.Reg.MergeReceivers[0].Ctx.Done():
		case tc.proc.Reg.MergeReceivers[0].Ch <- bat0:
			bat0.AddCnt(1)
		}
		select {
		case <-tc.proc.Reg.MergeReceivers[0].Ctx.Done():
		case tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}:
		}
		select {
		case <-tc.proc.Reg.MergeReceivers[0].Ctx.Done():
		case tc.proc.Reg.MergeReceivers[0].Ch <- nil:
		}
		bat1 := newBatch(t, tc.types, tc.proc, Rows)
		select {
		case <-tc.proc.Reg.MergeReceivers[0].Ctx.Done():
		case tc.proc.Reg.MergeReceivers[0].Ch <- bat1:
			bat1.AddCnt(1)
		}
		select {
		case <-tc.proc.Reg.MergeReceivers[0].Ctx.Done():
		case tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}:
		}
		select {
		case <-tc.proc.Reg.MergeReceivers[0].Ctx.Done():
		case tc.proc.Reg.MergeReceivers[0].Ch <- nil:
		}
		wg.Wait()
		bat0.Clean(tc.proc.Mp())
		bat1.Clean(tc.proc.Mp())
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func newTestCase(limit uint64, testTypes []types.Type) limitTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc := process.NewFromProc(testutil.NewProcessWithMPool(mpool.MustNewZero()),
		ctx, regNum)
	return limitTestCase{
		proc:  proc,
		types: testTypes,
		arg: &Argument{
			Limit:          limit,
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

func minValue(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
