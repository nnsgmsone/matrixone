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

package projection

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("projection(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitByTypes(ap.Types, proc)
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

	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)
	ap := arg.(*Argument)
	ap.ctr.OutBat.Reset()
	for i, e := range ap.Es {
		vec, err := colexec.EvalExpr(bat, proc, e)
		if err != nil {
			return false, err
		}
		needFree := true
		for i := range bat.Vecs {
			if vec == bat.Vecs[i] {
				needFree = false
			}
		}
		uf := ap.ctr.Ufs[i]
		len := vec.Length()
		for j := 0; j < len; j++ {
			if err := uf(ap.ctr.OutBat.Vecs[i], vec, int64(j)); err != nil {
				if needFree {
					vec.Free(proc.Mp())
				}
				return false, err
			}
		}
		if needFree {
			anal.Alloc(int64(vec.Size()))
			vec.Free(proc.Mp())
		}
	}
	ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, bat.Zs...)
	proc.SetInputBatch(ap.ctr.OutBat)
	anal.Output(ap.ctr.OutBat, isLast)
	return false, nil
}
