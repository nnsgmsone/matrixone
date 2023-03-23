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

package loopmark

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state int
	bat   *batch.Batch
	colexec.MemforNextOp
	probeFunc func(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error
}

type Argument struct {
	ctr    *container
	Cond   *plan.Expr
	Types  []types.Type
	Result []int32
}

func (ap *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ap.ctr.CleanMemForNextOp(proc)
	if ap.ctr.bat != nil {
		ap.ctr.bat.SubCnt(1)
		ap.ctr.bat = nil
	}
}
