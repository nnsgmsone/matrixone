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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	bat  *batch.Batch
	vecs []*vector.Vector
	ufs  []func(*vector.Vector, *vector.Vector, int64) error
}

// Argument pipe connector
type Argument struct {
	Types []types.Type // output vector types
	Reg   *process.WaitRegister
	ctr   *container
}

func (ap *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if pipelineFailed {
		for len(ap.Reg.Ch) > 0 {
			<-ap.Reg.Ch
		}
	}
	select {
	case ap.Reg.Ch <- nil:
	case <-ap.Reg.Ctx.Done():
	}
	if ap.ctr.bat != nil {
		ap.ctr.bat.Clean(proc.Mp())
		ap.ctr.bat = nil
	}

}
