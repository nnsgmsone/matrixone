// Copyright 2023 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (pm *PrivMem) InitByTypes(typs []types.Type, proc *process.Process) error {
	pm.OutBat = batch.NewWithSize(len(typs))
	pm.OutVecs = make([]*vector.Vector, len(typs))
	pm.Ufs = make([]func(*vector.Vector, *vector.Vector, int64) error, len(typs))
	for i := range typs {
		vec := vector.New(typs[i])
		vector.PreAlloc(vec, 0, defines.DefaultVectorSize, proc.Mp())
		pm.OutVecs[i] = vec
		pm.OutBat.SetVector(int32(i), vec)
		pm.Ufs[i] = vector.GetUnionOneFunction(typs[i], proc.Mp())
	}

	return nil
}

func (pm *PrivMem) Clean(proc *process.Process) error {
	if pm.OutBat != nil {
		pm.OutBat.Clean(proc.Mp())
		pm.OutBat = nil
	}
	return nil
}
