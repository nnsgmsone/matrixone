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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.childrenCount = ap.ChildrenNumber
	ap.ctr.InitByTypes(ap.Types, proc)
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
		ap.ctr.OutBat.Reset()
		for i, vec := range ap.ctr.OutVecs {
			uf := ap.ctr.Ufs[i]
			srcVec := bat.GetVector(int32(i))
			for j := int64(0); j < int64(length); j++ {
				if err := uf(vec, srcVec, j); err != nil {
					bat.SubCnt(1)
					return false, err
				}
			}
		}
		ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, bat.Zs...)
		anal.Input(bat, isFirst)
		bat.SubCnt(1)
		anal.Output(ap.ctr.OutBat, isLast)
		proc.SetInputBatch(ap.ctr.OutBat)
		return false, nil
	}
}
