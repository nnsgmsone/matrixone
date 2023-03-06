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

package mergelimit

import (
	"bytes"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", ap.Limit))
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.childrenCount = ap.ChildrenNumber
	ap.ctr.pm.InitByTypes(ap.Types, proc)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ctr := ap.ctr

	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		anal.WaitStop(start)

		if bat == nil {
			ctr.childrenCount--
			continue
		}

		if bat.Length() == 0 {
			continue
		}

		anal.Input(bat, isFirst)
		if ap.ctr.seen >= ap.Limit {
			proc.SetInputBatch(nil)
			return true, nil
		}

		newSeen := ap.ctr.seen + uint64(bat.Length())
		if newSeen > ap.Limit {
			ap.ctr.pm.OutBat.Reset()
			count := int64(ap.Limit - ap.ctr.seen)
			for i, vec := range ap.ctr.pm.OutVecs {
				uf := ap.ctr.pm.Ufs[i]
				srcVec := bat.GetVector(int32(i))
				for j := int64(0); j < count; j++ {
					if err := uf(vec, srcVec, j); err != nil {
						return false, err
					}
				}
				ap.ctr.pm.OutBat.Zs = append(ap.ctr.pm.OutBat.Zs, bat.Zs[:count]...)
			}
			ap.ctr.seen = newSeen
			anal.Output(ap.ctr.pm.OutBat, isLast)
			proc.SetInputBatch(ap.ctr.pm.OutBat)
			return true, nil
		}
		ap.ctr.seen = newSeen
		anal.Output(bat, isLast)
		return false, nil
	}
}
