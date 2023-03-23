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

package loopleft

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" loop left join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
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
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				return false, err
			}
			ctr.state = Probe

		case Probe:
			start := time.Now()
			bat := <-proc.Reg.MergeReceivers[0].Ch
			anal.WaitStop(start)

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				bat.SubCnt(1)
				continue
			}

			err := ctr.probeFunc(bat, ap, proc, anal, isFirst, isLast)
			bat.SubCnt(1)
			return false, err

		default:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	start := time.Now()
	bat := <-proc.Reg.MergeReceivers[1].Ch
	anal.WaitStop(start)

	if bat != nil {
		ctr.bat = bat
		ctr.probeFunc = ctr.probe
	} else {
		ctr.probeFunc = ctr.emptyProbe
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	ctr.OutBat.Reset()

	len := bat.Length()
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			for j := 0; j < len; j++ {
				uf := ctr.Ufs[i]
				if err := uf(ctr.OutBat.Vecs[i], bat.Vecs[rp.Pos], int64(j)); err != nil {
					return err
				}
			}
		} else {
			if err := vector.SetConstNull(ctr.OutBat.Vecs[i], len, proc.Mp()); err != nil {
				return err
			}
		}
	}
	ctr.OutBat.Zs = append(ctr.OutBat.Zs, bat.Zs...)
	anal.Output(ctr.OutBat, isLast)
	proc.SetInputBatch(ctr.OutBat)
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	ctr.OutBat.Reset()
	count := bat.Length()
	for i := 0; i < count; i++ {
		matched := false
		vec, err := colexec.JoinFilterEvalExpr(bat, ctr.bat, i, proc, ap.Cond)
		if err != nil {
			return err
		}
		bs := vector.MustFixedCol[bool](vec)
		if len(bs) == 1 {
			if bs[0] {
				for j := 0; j < len(ctr.bat.Zs); j++ {
					matched = true
					for k, rp := range ap.Result {
						uf := ctr.Ufs[k]
						if rp.Rel == 0 {
							if err := uf(ctr.OutBat.Vecs[k], bat.Vecs[rp.Pos], int64(i)); err != nil {
								vec.Free(proc.Mp())
								return err
							}
						} else {
							if err := uf(ctr.OutBat.Vecs[k], bat.Vecs[rp.Pos], int64(j)); err != nil {
								vec.Free(proc.Mp())
								return err
							}
						}
					}
					ctr.OutBat.Zs = append(ctr.OutBat.Zs, ctr.bat.Zs[j])
				}
			}
		} else {
			for j, b := range bs {
				if b {
					matched = true
					for k, rp := range ap.Result {
						uf := ctr.Ufs[k]
						if rp.Rel == 0 {
							if err := uf(ctr.OutBat.Vecs[k], bat.Vecs[rp.Pos], int64(i)); err != nil {
								vec.Free(proc.Mp())
								return err
							}
						} else {
							if err := uf(ctr.OutBat.Vecs[k], bat.Vecs[rp.Pos], int64(j)); err != nil {
								vec.Free(proc.Mp())
								return err
							}
						}
					}
					ctr.OutBat.Zs = append(ctr.OutBat.Zs, ctr.bat.Zs[j])
				}
			}
		}
		vec.Free(proc.Mp())
		if !matched {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					uf := ctr.Ufs[k]
					if err := uf(ctr.OutBat.Vecs[k], bat.Vecs[rp.Pos], int64(i)); err != nil {
						return err
					}
				} else {
					if err := ctr.OutBat.Vecs[k].UnionNull(proc.Mp()); err != nil {
						return err
					}
				}
			}
			ctr.OutBat.Zs = append(ctr.OutBat.Zs, bat.Zs[i])
		}
	}
	ctr.OutBat.ExpandNulls()
	anal.Output(ctr.OutBat, isLast)
	proc.SetInputBatch(ctr.OutBat)
	return nil
}
