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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Eval
	End
)

const (
	H0 = iota
	H8
	HStr
)

type container struct {
	state     int
	typ       int
	inserted  []uint8
	zInserted []uint8

	chunkInserted []uint8
	chunkValues   []uint64

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap

	isEmpty       bool // Indicates if it is an empty table
	pmIdx         int
	childrenCount int
	pms           []*colexec.MemforNextOp
}

type Argument struct {
	NeedEval       bool // need to projection the aggregate column
	ChildrenNumber int
	Types          []types.Type
	ctr            *container
}

func (ap *Argument) ReturnTypes() []types.Type {
	return ap.Types
}

func (ap *Argument) Free(proc *process.Process, pipelineFailed bool) {
	for i := range ap.ctr.pms {
		ap.ctr.pms[i].CleanMemForNextOp(proc)
	}
	ap.ctr.cleanHashMap()
}

func (ctr *container) cleanHashMap() {
	if ctr.intHashMap != nil {
		ctr.intHashMap.Free()
		ctr.intHashMap = nil
	}
	if ctr.strHashMap != nil {
		ctr.strHashMap.Free()
		ctr.strHashMap = nil
	}
}
