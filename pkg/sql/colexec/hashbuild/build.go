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

package hashbuild

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
	buf.WriteString(" hash build ")
}

func Prepare(proc *process.Process, arg any) error {
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	if ap.NeedHashMap {
		if ap.ctr.mp, err = hashmap.NewStrMap(false, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
			return err
		}
		ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions))
		ap.ctr.evecs = make([]evalVector, len(ap.Conditions))
		ap.ctr.nullSels = make([]int32, 0)
	}
	ap.ctr.InitByTypes(ap.Types, proc)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, _ bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal, isFirst); err != nil {
				return false, err
			}
			if ctr.mp != nil {
				anal.Alloc(ctr.mp.Size())
			}
			ctr.state = End
		case End:
			if ctr.OutBat.Length() != 0 {
				if ap.NeedHashMap {
					ctr.OutBat.Ht = hashmap.NewJoinMap(ctr.sels, ctr.nullSels, nil, ctr.mp, ctr.hasNull)
				}
				proc.SetInputBatch(ctr.OutBat)
				ctr.mp = nil
				ctr.sels = nil
				ctr.nullSels = nil
			} else {
				proc.SetInputBatch(nil)
			}
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var err error

	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		anal.WaitStop(start)
		if bat == nil {
			break
		}
		if bat.Length() == 0 {
			bat.SubCnt(1)
			continue
		}
		anal.Input(bat, isFirst)
		anal.Alloc(int64(bat.Size()))
		for i, vec := range ctr.OutVecs {
			uf := ctr.Ufs[i]
			srcVec := bat.GetVector(int32(i))
			for j := int64(0); j < int64(bat.Length()); j++ {
				if err := uf(vec, srcVec, j); err != nil {
					bat.SubCnt(1)
					return nil
				}
			}
		}
		ctr.OutBat.Zs = append(ctr.OutBat.Zs, bat.Zs...)
		bat.SubCnt(1)
	}
	if ctr.OutBat.Length() == 0 || !ap.NeedHashMap {
		return nil
	}
	if err = ctr.evalJoinCondition(ctr.OutBat, ap.Conditions, proc, anal); err != nil {
		return err
	}
	defer ctr.cleanEvalVectors(proc.Mp())
	inBuckets := make([]uint8, hashmap.UnitLimit)
	itr := ctr.mp.NewIterator()
	count := ctr.OutBat.Length()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rows := ctr.mp.GroupCount()
		vals, zvals, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if zvals[k] == 0 {
				ctr.hasNull = true
				continue
			}
			if v == 0 {
				continue
			}
			if v > rows {
				ctr.sels = append(ctr.sels, make([]int32, 0))
			}
			ai := int64(v) - 1
			ctr.sels[ai] = append(ctr.sels[ai], int32(i+k))
		}
		if ap.IsRight {
			copy(inBuckets, hashmap.OneUInt8s)
			_, zvals = itr.Find(i, n, ctr.vecs, inBuckets)
			for k := 0; k < n; k++ {
				if inBuckets[k] == 0 {
					continue
				}
				if zvals[k] == 0 {
					ctr.nullSels = append(ctr.nullSels, int32(i+k))
				}
			}
		}
	}
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
