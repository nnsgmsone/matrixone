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

package offset

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var emptyBatch = &batch.Batch{}

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("offset(%v)", ap.Offset))
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.pm = new(colexec.PrivMem)
	ap.ctr.pm.InitByTypes(ap.Types, proc)
	return nil
}

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

	length := bat.Length()
	if ap.ctr.seen > ap.Offset {
		proc.SetInputBatch(bat)
		return false, nil
	}
	if ap.ctr.seen+uint64(length) > ap.Offset {
		ap.ctr.pm.Bat.Reset()
		start, count := int64(ap.Offset-ap.ctr.seen), int64(length)-int64(ap.Offset-ap.ctr.seen)
		for i, vec := range ap.ctr.pm.Vecs {
			uf := ap.ctr.pm.Ufs[i]
			srcVec := bat.GetVector(int32(i))
			for j := int64(0); j < count; j++ {
				if err := uf(vec, srcVec, j+start); err != nil {
					return false, err
				}
			}
		}
		for i := int64(0); i < count; i++ {
			ap.ctr.pm.Bat.Zs = append(ap.ctr.pm.Bat.Zs, i+start)
		}
		proc.SetInputBatch(ap.ctr.pm.Bat)
		return false, nil
	}
	ap.ctr.seen += uint64(length)
	proc.SetInputBatch(emptyBatch)
	return false, nil
}
