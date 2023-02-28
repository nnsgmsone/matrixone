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

package merge

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.childrenCount = ap.ChildrenNumber
	ap.ctr.bat = batch.NewWithSize(len(ap.Types))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Types))
	for i := range ap.Types {
		vec := vector.New(ap.Types[i])
		vector.PreAlloc(vec, 0, defines.DefaultVectorSize, proc.Mp())
		ap.ctr.vecs[i] = vec
		ap.ctr.bat.SetVector(int32(i), vec)
	}
	ap.ctr.ufs = make([]func(*vector.Vector, *vector.Vector, int64) error, len(ap.Types))
	for i := range ap.Types {
		ap.ctr.ufs[i] = vector.GetUnionOneFunction(ap.Types[i], proc.Mp())
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr

	for {
		if ctr.childrenCount == 0 {
			proc.SetInputBatch(nil)
			return true, nil
		}

		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		if bat == nil {
			ctr.childrenCount--
			continue
		}
		anal.WaitStop(start)
		length := bat.Length()
		if length == 0 {
			bat.SubCnt(1)
			continue
		}
		ap.ctr.bat.Reset()
		for i, vec := range ap.ctr.vecs {
			uf := ap.ctr.ufs[i]
			srcVec := bat.GetVector(int32(i))
			for j := int64(0); j < int64(length); j++ {
				if err := uf(vec, srcVec, j); err != nil {
					bat.SubCnt(1)
					return false, err
				}
			}
		}
		ap.ctr.bat.Zs = append(ap.ctr.bat.Zs, bat.Zs...)
		anal.Input(bat, isFirst)
		bat.SubCnt(1)
		anal.Output(ap.ctr.bat, isLast)
		proc.SetInputBatch(ap.ctr.bat)
		return false, nil
	}
}
