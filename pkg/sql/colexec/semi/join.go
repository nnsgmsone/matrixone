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

package semi

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" semi join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.eligible = make([]int64, hashmap.UnitLimit)
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
				return false, err
			}
			ctr.state = Probe

		case Probe:
			start := time.Now()
			bat := <-proc.Reg.MergeReceivers[0].Ch
			anal.WaitStop(start)

			if bat == nil {
				ctr.bat.SubCnt(1)
				ctr.state = End
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
		ctr.mp = bat.Ht.(*hashmap.JoinMap).Dup()
		anal.Alloc(ctr.mp.Map().Size())
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	ctr.OutBat.Reset()
	defer ctr.cleanEvalVectors(proc.Mp())
	if err := ctr.evalJoinCondition(bat, ap.Conditions[0], proc, anal); err != nil {
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
		ctr.resetProbeRelatedMembers()
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			if ap.Cond != nil {
				matched := false // mark if any tuple satisfies the condition
				sels := mSels[vals[k]-1]
				for _, sel := range sels {
					vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i+k, int(sel), proc, ap.Cond)
					if err != nil {
						return err
					}
					bs := vector.MustFixedCol[bool](vec)
					if bs[0] {
						matched = true
						vec.Free(proc.Mp())
						break
					}
					vec.Free(proc.Mp())
				}
				if !matched {
					continue
				}
			}
			ctr.eligible = append(ctr.eligible, int64(i+k))
			ctr.OutBat.Zs = append(ctr.OutBat.Zs, bat.Zs[i+k])
		}
		for j, pos := range ap.Result {
			uf := ctr.Ufs[pos]
			for _, e := range ctr.eligible {
				if err := uf(ctr.OutVecs[j], bat.Vecs[pos], e); err != nil {
					return err
				}
			}
		}
	}
	ctr.OutBat.ExpandNulls()
	anal.Output(ctr.OutBat, isLast)
	proc.SetInputBatch(ctr.OutBat)
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process, analyze process.Analyze) error {
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
		if ctr.evecs[i].needFree && vec != nil {
			analyze.Alloc(int64(vec.Size()))
		}
	}
	return nil
}

func (ctr *container) resetProbeRelatedMembers() {
	copy(ctr.inBuckets, hashmap.OneUInt8s)
	copy(ctr.eligible, hashmap.OneInt64s)
}
