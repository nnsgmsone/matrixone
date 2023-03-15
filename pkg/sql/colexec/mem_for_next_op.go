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

func (m *MemforNextOp) InitByTypes(typs []types.Type, proc *process.Process) error {
	m.OutBat = batch.NewWithSize(len(typs))
	m.OutVecs = make([]*vector.Vector, len(typs))
	m.Ufs = make([]func(*vector.Vector, *vector.Vector, int64) error, len(typs))
	for i := range typs {
		vec := vector.NewVec(typs[i])
		vec.PreExtend(defines.DefaultVectorRows, proc.Mp())
		m.OutVecs[i] = vec
		m.OutBat.SetVector(int32(i), vec)
		m.Ufs[i] = vector.GetUnionOneFunction(typs[i], proc.Mp())
	}
	return nil
}

func (m *MemforNextOp) Dup(proc *process.Process) (*MemforNextOp, error) {
	dupm := new(MemforNextOp)
	dupm.OutBat = batch.NewWithSize(len(m.Ufs))
	dupm.OutVecs = make([]*vector.Vector, len(m.Ufs))
	dupm.Ufs = make([]func(*vector.Vector, *vector.Vector, int64) error, len(m.Ufs))
	for i := range m.Ufs {
		vec := vector.NewVec(*m.OutVecs[i].GetType())
		vec.PreExtend(defines.DefaultVectorRows, proc.Mp())
		dupm.OutVecs[i] = vec
		dupm.OutBat.SetVector(int32(i), vec)
		dupm.Ufs[i] = m.Ufs[i]
	}
	return dupm, nil
}

func (m *MemforNextOp) CleanMemForNextOp(proc *process.Process) error {
	if m.OutBat != nil {
		m.OutBat.Clean(proc.Mp())
		m.OutBat = nil
	}
	return nil
}
