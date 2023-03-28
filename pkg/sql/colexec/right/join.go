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

package right

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" right join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
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
				ap.Free(proc, true)
				return false, err
			}
			ctr.state = Probe

		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = SendLast
				continue
			}
			if bat.Length() == 0 {
				bat.SubCnt(1)
				continue
			}

			if ctr.bat == nil || ctr.bat.Length() == 0 {
				bat.SubCnt(1)
				continue
			}

			err := ctr.probe(bat, ap, proc, anal, isFirst, isLast)
			bat.SubCnt(1)
			return false, err

		case SendLast:
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				ctr.state = End
			} else {
				setNil, err := ctr.emptyProbe(ap, proc, anal, isFirst, isLast)
				if err != nil {
					return false, err
				}

				ctr.state = End
				if setNil {
					continue
				}
				return false, nil
			}

		default:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.Ht.(*hashmap.JoinMap).Dup()
	}
	return nil
}

func (ctr *container) emptyProbe(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (bool, error) {
	msel := ctr.mp.Sels()
	unmatch := make([]int32, 0)
	matched_sels := make(map[int32]bool)

	if !ap.Is_receiver {
		ap.Channel <- &ctr.matched_sels
		//goto ctr.end directly
		return true, nil
	}

	for _, value := range ctr.matched_sels {
		matched_sels[value] = true
	}

	if ap.NumCPU > 1 {
		cnt := 1
		for v := range ap.Channel {
			for _, value := range *v {
				matched_sels[value] = true
			}
			cnt++
			if cnt == int(ap.NumCPU) {
				close(ap.Channel)
				break
			}
		}
	}

	for _, sel := range msel {
		for _, value := range sel {
			if !matched_sels[value] {
				unmatch = append(unmatch, value)
			}
		}
	}
	unmatch = append(unmatch, ctr.mp.Nullsels()...)

	count := len(unmatch)
	if count == 0 {
		return true, nil
	}

	ctr.OutBat.Reset()
	for i := 0; i < count; i++ {
		for j, rp := range ap.Result {
			if rp.Rel == 0 {
				if err := ctr.OutBat.Vecs[j].UnionNull(proc.Mp()); err != nil {
					return false, err
				}
			} else {
				uf := ctr.Ufs[j]
				if err := uf(ctr.OutBat.Vecs[j], ctr.bat.Vecs[rp.Pos], int64(unmatch[i])); err != nil {
					return false, err
				}

			}

		}
		ctr.OutBat.Zs = append(ctr.OutBat.Zs, ctr.bat.Zs[unmatch[i]])
	}
	ctr.OutBat.ExpandNulls()
	anal.Output(ctr.OutBat, isLast)
	proc.SetInputBatch(ctr.OutBat)
	return false, nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	defer bat.Clean(proc.Mp())
	anal.Input(bat, isFirst)
	ctr.OutBat.Reset()

	ctr.cleanEvalVectors(proc.Mp())
	if err := ctr.evalJoinCondition(bat, ap.Conditions[0], proc); err != nil {
		return err
	}

	count := bat.Length()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.Map().NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			sels := mSels[vals[k]-1]
			for _, sel := range sels {
				if ap.Cond != nil {
					vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i+k, int(sel), proc, ap.Cond)
					if err != nil {
						return err
					}
					bs := vector.MustFixedCol[bool](vec)
					if !bs[0] {
						vec.Free(proc.Mp())
						continue
					}
					vec.Free(proc.Mp())
				}
				for j, rp := range ap.Result {
					uf := ctr.Ufs[j]
					if rp.Rel == 0 {
						if err := uf(ctr.OutBat.Vecs[j], bat.Vecs[rp.Pos], int64(i+k)); err != nil {
							return err
						}
					} else {
						if err := uf(ctr.OutBat.Vecs[j], bat.Vecs[rp.Pos], int64(sel)); err != nil {
							return err
						}
					}
				}
				ctr.matched_sels = append(ctr.matched_sels, sel)
				ctr.OutBat.Zs = append(ctr.OutBat.Zs, ctr.bat.Zs[sel])
			}
		}
	}
	ctr.OutBat.ExpandNulls()
	anal.Output(ctr.OutBat, isLast)
	proc.SetInputBatch(ctr.OutBat)
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
		ctr.evecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.evecs[i].needFree = false
				break
			}
		}
	}
	return nil
}
