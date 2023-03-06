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

package limit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", ap.Limit))
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.InitByTypes(ap.Types, proc)
	return nil
}

// Call returning only the first n tuples from its input
func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)
	if ap.ctr.seen >= ap.Limit {
		proc.SetInputBatch(nil)
		return true, nil
	}
	length := bat.Length()
	newSeen := ap.ctr.seen + uint64(length)

	if newSeen >= ap.Limit {
		ap.ctr.OutBat.Reset()
		count := int64(ap.Limit - ap.ctr.seen)
		for i, vec := range ap.ctr.OutVecs {
			uf := ap.ctr.Ufs[i]
			srcVec := bat.GetVector(int32(i))
			for j := int64(0); j < count; j++ {
				if err := uf(vec, srcVec, j); err != nil {
					return false, err
				}
			}
			ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, bat.Zs[:count]...)
		}
		ap.ctr.seen = newSeen
		anal.Output(ap.ctr.OutBat, isLast)
		proc.SetInputBatch(ap.ctr.OutBat)
		return true, nil
	}
	anal.Output(bat, isLast)
	ap.ctr.seen = newSeen
	return false, nil
}
