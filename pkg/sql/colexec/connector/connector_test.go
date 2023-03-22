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

package connector

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
	Rows           = 10 // default rows
	BenchmarkCount = 100
)

// add unit tests for cases
type connectorTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []connectorTestCase
)

func init() {
	tcs = []connectorTestCase{
		newTestCase(),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestConnector(t *testing.T) {
	for _, tc := range tcs {
		go func() { // simulated consumers
			for {
				bat := <-tc.proc.Reg.MergeReceivers[0].Ch
				if bat == nil {
					break
				}
				bat.SubCnt(1)
			}
		}()
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

func BenchmarkConnector(b *testing.B) {
	t := new(testing.T)
	for i := 0; i < 1000; i++ {
		tc := newTestCase()
		go func() { // simulated consumers
			for {
				bat := <-tc.proc.Reg.MergeReceivers[0].Ch
				if bat == nil {
					break
				}
				bat.SubCnt(1)
			}
		}()
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		b.ResetTimer()
		for i := 0; i < BenchmarkCount; i++ {
			bat := newBatch(t, tc.types, tc.proc, Rows)
			tc.proc.SetInputBatch(bat)
			_, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			bat.Clean(tc.proc.Mp())
		}
		tc.proc.SetInputBatch(nil)
		_, _ = Call(0, tc.proc, tc.arg, false, false)
		tc.arg.Free(tc.proc, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func newTestCase() connectorTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc := process.NewFromProc(testutil.NewProcessWithMPool(mpool.MustNewZero()),
		ctx, 1)
	return connectorTestCase{
		proc: proc,
		types: []types.Type{
			types.New(types.T_int8, 0, 0),
		},
		arg: &Argument{
			Reg: proc.Reg.MergeReceivers[0],
			Types: []types.Type{
				types.New(types.T_int8, 0, 0),
			},
		},
		cancel: cancel,
	}

}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
