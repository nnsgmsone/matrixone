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

package mergeoffset

import (
	"bytes"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeOffset(%d)", ap.Offset))
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.childrenCount = ap.ChildrenNumber
	ap.ctr.InitByTypes(ap.Types, proc)
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ctr := ap.ctr
	for {
		if ctr.childrenCount == 0 {
			proc.SetInputBatch(nil)
			return true, nil
		}
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		anal.WaitStop(start)
		if bat == nil {
			ctr.childrenCount--
			continue
		}
		if bat.Length() == 0 {
			bat.SubCnt(1)
			continue
		}
		anal.Input(bat, isFirst)
		if ap.ctr.seen >= ap.Offset {
			ap.ctr.OutBat.Reset()
			for i, vec := range ap.ctr.OutVecs {
				uf := ap.ctr.Ufs[i]
				srcVec := bat.GetVector(int32(i))
				for j := int64(0); j < int64(bat.Length()); j++ {
					if err := uf(vec, srcVec, j); err != nil {
						bat.SubCnt(1)
						return false, err
					}
				}
			}
			bat.SubCnt(1)
			ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, bat.Zs...)
			proc.SetInputBatch(ap.ctr.OutBat)
			anal.Output(ap.ctr.OutBat, isLast)
			return false, nil
		}
		length := len(bat.Zs)
		// bat = PartOne + PartTwo, and PartTwo is required.
		newSeen := ap.ctr.seen + uint64(length)
		if newSeen > ap.Offset {
			ap.ctr.OutBat.Reset()
			start, count := int64(ap.Offset-ap.ctr.seen), int64(length)-int64(ap.Offset-ap.ctr.seen)
			for i, vec := range ap.ctr.OutVecs {
				uf := ap.ctr.Ufs[i]
				srcVec := bat.GetVector(int32(i))
				for j := int64(0); j < count; j++ {
					if err := uf(vec, srcVec, j+start); err != nil {
						return false, err
					}
				}
			}
			for i := int64(0); i < count; i++ {
				ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, i+start)
			}
			proc.SetInputBatch(ap.ctr.OutBat)
			anal.Output(ap.ctr.OutBat, isLast)
			ap.ctr.seen = newSeen
			return false, nil
		}
		ap.ctr.seen = newSeen
	}
}
