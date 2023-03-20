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

package mergeorder

import (
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Eval
	End
)

type container struct {
	n     int // result vector number
	state int
	poses []int32           // sorted list of attributes
	cmps  []compare.Compare // compare structures used to do sort work for attrs

	// some reused memory
	unionFlag                    []uint8
	compare0Index, compare1Index []int32
	finalSelectList              []int64

	init          bool // means that it has been initialized
	childrenCount int
	colexec.MemforNextOp
}

type Argument struct {
	ctr            *container // ctr stores the attributes needn't do Serialization work
	ChildrenNumber int
	Fs             []*plan.OrderBySpec // Fields store the order information
	// output vector types
	Types []types.Type
}

func (ap *Argument) Free(proc *process.Process, pipelineFailed bool) {
	for len(proc.Reg.MergeReceivers[0].Ch) > 0 {
		<-proc.Reg.MergeReceivers[0].Ch
	}
	ap.ctr.CleanMemForNextOp(proc)
}
