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

	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)
	ap := arg.(*Argument)
	if len(ap.Es) > 0 {
		for i, e := range ap.Es {
			vec, err := colexec.EvalExpr(bat, proc, e)
			if err != nil || vec.ConstExpand(false, proc.Mp()) == nil {
				return false, err
			}
			uf := ap.ctr.pm.Ufs[i]
			len := vec.Length()
			for j := 0; j < len; j++ {
				if err := uf(ap.ctr.pm.Bat.Vecs[i], vec, int64(j)); err != nil {
					return false, err
				}
			}
			ap.ctr.pm.Bat.Zs = append(ap.ctr.pm.Bat.Zs, bat.Zs...)
		}
		proc.SetInputBatch(ap.ctr.pm.Bat)
		anal.Output(ap.ctr.pm.Bat, isLast)
	}
	return false, nil
}
