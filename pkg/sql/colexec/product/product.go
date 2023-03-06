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

package product

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" cross join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.pm.InitByTypes(ap.Typs, proc)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				ap.Free(proc, true)
				return false, err
			}
			ctr.state = Probe

		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				continue
			}
			if len(bat.Zs) == 0 {
				continue
			}

			if err := ctr.probe(bat, ap, proc, anal, isFirst, isLast); err != nil {
				bat.Clean(proc.Mp())
				ap.Free(proc, true)
				return false, err
			}
			return false, nil

		default:
			ap.Free(proc, false)
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	defer bat.Clean(proc.Mp())
	anal.Input(bat, isFirst)
	ctr.pm.OutBat.Reset()
	count := bat.Length()
	for i := 0; i < count; i++ {
		for j := 0; j < len(ctr.bat.Zs); j++ {
			for k, rp := range ap.Result {
				uf := ctr.pm.Ufs[k]
				if rp.Rel == 0 {
					if err := uf(ctr.pm.OutBat.Vecs[k], bat.Vecs[rp.Pos], int64(i)); err != nil {
						return err
					}
				} else {
					if err := uf(ctr.pm.OutBat.Vecs[k], ctr.bat.Vecs[rp.Pos], int64(j)); err != nil {
						return err
					}
				}
			}
			ctr.pm.OutBat.Zs = append(ctr.pm.OutBat.Zs, ctr.bat.Zs[j])
		}
	}
	anal.Output(ctr.pm.OutBat, isLast)
	proc.SetInputBatch(ctr.pm.OutBat)
	return nil
}
