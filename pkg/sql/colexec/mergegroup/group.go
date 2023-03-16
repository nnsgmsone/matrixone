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

package mergegroup

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("mergeroup()")
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.isEmpty = true
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.chunkInserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.chunkValues = make([]uint64, hashmap.UnitLimit)
	ap.ctr.pms = make([]*colexec.PrivMem, 1)
	ap.ctr.pms[0] = new(colexec.PrivMem)
	ap.ctr.pms[0].InitByTypes(ap.Types, proc)
	ap.ctr.childrenCount = ap.ChildrenNumber
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}, isFirst bool, isLast bool) (bool, error) {
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
			ctr.state = Eval
		case Eval:
			pm := ctr.pms[ctr.pmIdx]
			if ap.NeedEval {
				for i, ag := range pm.Bat.Aggs {
					vec, err := ag.Eval(proc.Mp())
					if err != nil {
						ctr.state = End
						return false, err
					}
					pm.Bat.Aggs[i] = nil
					pm.Bat.Vecs = append(pm.Bat.Vecs, vec)
					anal.Alloc(int64(vec.Size()))
				}
				for i := range pm.Bat.Zs {
					pm.Bat.Zs[i] = 1
				}
			}
			anal.Output(pm.Bat, isLast)
			proc.SetInputBatch(pm.Bat)
			ctr.pmIdx--
			if ctr.pmIdx == -1 {
				ctr.state = End
				return true, nil
			}
			return false, nil
		case End:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	for {
		if ctr.childrenCount == 0 {
			return nil
		}
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		if bat == nil {
			ctr.childrenCount--
			continue
		}
		anal.WaitStop(start)
		length := bat.Length()
		if length == 0 {
			bat.SubCnt(1)
			continue
		}
		if err := ctr.process(ap, bat, proc); err != nil {
			bat.SubCnt(1)
			return err
		}
		bat.SubCnt(1)
	}
}

func (ctr *container) process(ap *Argument, bat *batch.Batch, proc *process.Process) error {
	var err error

	if ctr.isEmpty {
		size := 0
		ctr.isEmpty = false
		for _, vec := range bat.Vecs {
			switch vec.GetType().TypeSize() {
			case 1:
				size += 1 + 1
			case 2:
				size += 2 + 1
			case 4:
				size += 4 + 1
			case 8:
				size += 8 + 1
			case 16:
				size += 16 + 1
			default:
				size = 128
			}
		}
		ap.initAggInfo(bat.Aggs)
		switch {
		case size == 0:
			ctr.typ = H0
		case size <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(true, 0, 0, proc.Mp()); err != nil {
				return err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(true, 0, 0, proc.Mp()); err != nil {
				return err
			}
		}
	}
	if rows := ctr.pms[ctr.pmIdx].Bat.Length(); rows > defines.DefaultVectorRows {
		pm := new(colexec.PrivMem)
		ctr.pmIdx++
		pm.InitByTypes(ap.Types, proc)
		ctr.pms = append(ctr.pms, pm)
		pm.Bat.Aggs = make([]agg.Agg[any], len(ctr.pms[ctr.pmIdx-1].Bat.Aggs))
		ap.initAggInfo(ctr.pms[ctr.pmIdx-1].Bat.Aggs)
	}
	switch ctr.typ {
	case H0:
		err = ctr.processH0(bat, proc)
	case H8:
		err = ctr.processH8(bat, proc)
	default:
		err = ctr.processHStr(bat, proc)
	}
	if err != nil {
		return err
	}
	return nil
}

func (ctr *container) processH0(bat *batch.Batch, proc *process.Process) error {
	for _, z := range bat.Zs {
		ctr.pms[0].Bat.Zs[0] += z
	}
	for i, agg := range ctr.pms[0].Bat.Aggs {
		if err := agg.Merge(bat.Aggs[i], 0, 0); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.intHashMap.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rows := ctr.intHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		if err = ctr.batchFill(i, n, bat, vals, rows, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.strHashMap.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rowCount := ctr.strHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		if err := ctr.batchFill(i, n, bat, vals, rowCount, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) batchFill(i int, n int, bat *batch.Batch, vals []uint64, hashRows uint64, proc *process.Process) error {
	cnt := 0
	valCnt := 0
	pm := ctr.pms[ctr.pmIdx]
	copy(ctr.inserted[:n], ctr.zInserted[:n])
	for k, v := range vals {
		if v == 0 {
			continue
		}
		if v > hashRows {
			ctr.inserted[k] = 1
			hashRows++
			cnt++
			pm.Bat.Zs = append(pm.Bat.Zs, 0)
		}
		valCnt++
	}
	if cnt > 0 {
		for j, vec := range pm.Vecs {
			uf := pm.Ufs[j]
			srcVec := bat.GetVector(int32(j))
			for k, flg := range ctr.inserted[:n] {
				if flg > 0 {
					if err := uf(vec, srcVec, int64(i+k)); err != nil {
						return err
					}
				}
			}
		}
		for _, ag := range pm.Bat.Aggs {
			if err := ag.Grows(cnt, proc.Mp()); err != nil {
				return err
			}
		}
	}
	if valCnt == 0 {
		return nil
	}
	rows := int64(0)
	for _, pm0 := range ctr.pms {
		ctr.chunkValues = ctr.chunkValues[:0]
		ctr.chunkInserted = ctr.chunkInserted[:0]
		oldRows := rows
		rows += int64(pm.Bat.Length())
		for k, v := range vals[:n] {
			if v == 0 {
				continue
			}
			ai := int64(v) - 1
			if ai < rows && ai >= oldRows {
				pm.Bat.Zs[ai-oldRows] += bat.Zs[i+k]
				ctr.chunkValues = append(ctr.chunkValues, v)
				ctr.chunkInserted = append(ctr.chunkInserted, ctr.inserted[k])
			}
		}
		for j, ag := range pm0.Bat.Aggs {
			if err := ag.BatchMerge(bat.Aggs[j], int64(i), ctr.chunkInserted, ctr.chunkValues); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ap *Argument) initAggInfo(aggs []agg.Agg[any]) {
	for i, ag := range aggs {
		ap.ctr.pms[ap.ctr.pmIdx].Bat.Aggs[i] = ag.Dup()
	}
}
