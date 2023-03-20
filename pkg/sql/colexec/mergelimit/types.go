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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	colexec.MemforNextOp

	seen          uint64
	childrenCount int
}

type Argument struct {
	ChildrenNumber int
	// Limit records the limit number of this operator
	Limit uint64
	// output vector types
	Types []types.Type
	// ctr stores the attributes needn't do Serialization work
	ctr *container
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	for len(proc.Reg.MergeReceivers[0].Ch) > 0 {
		<-proc.Reg.MergeReceivers[0].Ch
	}
	arg.ctr.CleanMemForNextOp(proc)
}
