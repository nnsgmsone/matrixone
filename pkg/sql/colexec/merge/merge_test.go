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

package merge

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
	Rows = 10 // default rows
)

// add unit tests for cases
type mergeTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []mergeTestCase
)

func init() {
	tcs = []mergeTestCase{
		newTestCase(),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestMerge(t *testing.T) {
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
		bat0 := newBatch(t, tc.types, tc.proc, Rows)
		bat0.AddCnt(1)
		tc.proc.Reg.MergeReceivers[0].Ch <- bat0
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		bat1 := newBatch(t, tc.types, tc.proc, Rows)
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

func newTestCase() mergeTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc := process.NewFromProc(testutil.NewProcessWithMPool(mpool.MustNewZero()),
		ctx, 2)
	types := []types.Type{
		types.New(types.T_int8, 0, 0, 0),
	}
	return mergeTestCase{
		proc:  proc,
		types: types,
		arg: &Argument{
			Types:          types,
			ChildrenNumber: 2,
		},
		cancel: cancel,
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
