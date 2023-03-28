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

package order

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type container struct {
	sels      []int64
	desc      []bool // ds[i] == true: the attrs[i] are in descending order
	nullsLast []bool
	vecs      []evalVector // sorted list of attributes
	init      bool         // means that it has been initialized

	colexec.MemforNextOp
}

type Argument struct {
	ctr   *container
	Types []types.Type
	Fs    []*plan.OrderBySpec
}

func (ap *Argument) ReturnTypes() []types.Type {
	return ap.Types
}

func (ap *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ap.ctr.CleanMemForNextOp(proc)
}

func (ctr *container) cleanEvalVectors(mp *mpool.MPool) {
	for i := range ctr.vecs {
		if ctr.vecs[i].needFree && ctr.vecs[i].vec != nil {
			ctr.vecs[i].vec.Free(mp)
			ctr.vecs[i].vec = nil
		}
	}
}
