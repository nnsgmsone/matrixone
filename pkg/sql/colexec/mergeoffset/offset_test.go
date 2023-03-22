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
	regNum = 2  // test reg number
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

func TestOffset(t *testing.T) {
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

func newTestCase(offset uint64) offsetTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc := process.NewFromProc(testutil.NewProcessWithMPool(mpool.MustNewZero()),
		ctx, regNum)
	testTypes := []types.Type{{Oid: types.T_int8, Size: 1}}
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
