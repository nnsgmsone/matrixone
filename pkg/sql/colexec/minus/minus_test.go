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

package minus

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type minusTestCase struct {
	proc         *process.Process
	arg          *Argument
	cancel       context.CancelFunc
	leftBatches  []*batch.Batch
	rightBatches []*batch.Batch
}

func TestMinus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	proc := process.NewFromProc(testutil.NewProcessWithMPool(mpool.MustNewZero()),
		ctx, -1)
	// [2 rows + 2 row, 3 columns] minus [1 row + 1 rows, 3 columns]
	/*
		{1, 2, 3}	{1, 2, 3}
		{1, 2, 3} minus {4, 5, 6} ==> {3, 4, 5}
		{3, 4, 5}
		{3, 4, 5}
	*/
	tc := newMinusTestCase(
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()},
		proc, cancel,
		[]*batch.Batch{
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 1}),
					testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{2, 2}),
					testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{3, 3}),
				}, nil),
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{3, 3}),
					testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{4, 4}),
					testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{5, 5}),
				}, nil),
		},
		[]*batch.Batch{
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1}),
					testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{2}),
					testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{3}),
				}, nil),
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{4}),
					testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{5}),
					testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{6}),
				}, nil),
		},
	)
	var wg sync.WaitGroup

	err := Prepare(tc.proc, tc.arg)
	require.NoError(t, err)
	wg.Add(1)
	go func() {
		for {
			if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
				wg.Done()
				break
			}
		}
		tc.arg.Free(tc.proc, false)
	}()
	for i := range tc.rightBatches {
		bat := tc.rightBatches[i]
		bat.AddCnt(1)
		tc.proc.Reg.MergeReceivers[1].Ch <- bat
	}
	tc.proc.Reg.MergeReceivers[1].Ch <- nil
	for i := range tc.leftBatches {
		bat := tc.leftBatches[i]
		bat.AddCnt(1)
		tc.proc.Reg.MergeReceivers[0].Ch <- bat
	}
	tc.proc.Reg.MergeReceivers[0].Ch <- nil
	wg.Wait()
	for i := range tc.rightBatches {
		tc.rightBatches[i].Clean(proc.Mp())
	}
	for i := range tc.leftBatches {
		tc.leftBatches[i].Clean(proc.Mp())
	}
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func newMinusTestCase(typs []types.Type, proc *process.Process, cancel context.CancelFunc,
	leftBatches, rightBatches []*batch.Batch) minusTestCase {
	arg := &Argument{
		Types: typs,
	}
	return minusTestCase{
		proc:         proc,
		arg:          arg,
		cancel:       cancel,
		leftBatches:  leftBatches,
		rightBatches: rightBatches,
	}
}
