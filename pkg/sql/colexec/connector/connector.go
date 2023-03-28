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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.bat = batch.NewWithSize(len(ap.Types))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Types))
	for i := range ap.Types {
		vec := vector.NewVec(ap.Types[i])
		vec.PreExtend(defines.DefaultVectorRows, proc.Mp())
		ap.ctr.vecs[i] = vec
		ap.ctr.bat.SetVector(int32(i), vec)
	}
	ap.ctr.ufs = make([]func(*vector.Vector, *vector.Vector, int64) error, len(ap.Types))
	for i := range ap.Types {
		ap.ctr.ufs[i] = vector.GetUnionOneFunction(ap.Types[i], proc.Mp())
	}
	return nil
}

func Call(_ int, proc *process.Process, arg any, _ bool, _ bool) (bool, error) {
	ap := arg.(*Argument)
	reg := ap.Reg
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	length := bat.Length()
	if length == 0 {
		return false, nil
	}
	ap.ctr.bat.Reset()
	for i, vec := range ap.ctr.vecs {
		uf := ap.ctr.ufs[i]
		srcVec := bat.GetVector(int32(i))
		for j := int64(0); j < int64(length); j++ {
			if err := uf(vec, srcVec, j); err != nil {
				return false, err
			}
		}
	}
	ap.ctr.bat.Aggs = append(ap.ctr.bat.Aggs[:0], bat.Aggs...)
	ap.ctr.bat.Zs = append(ap.ctr.bat.Zs, bat.Zs...)
	select {
	case <-reg.Ctx.Done():
		return true, nil
	case reg.Ch <- ap.ctr.bat:
		ap.ctr.bat.AddCnt(1)
		proc.SetInputBatch(nil)
		return false, nil
	}
}
