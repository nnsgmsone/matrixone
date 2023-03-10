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

package group

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_col/group_concat"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("group([")
	for i, expr := range ap.Exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString("], [")
	for i, ag := range ap.Aggs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v(%v)", agg.Names[ag.Op], ag.E))
	}
	if len(ap.MultiAggs) != 0 {
		if len(ap.Aggs) > 0 {
			buf.WriteString(",")
		}
		for i, ag := range ap.MultiAggs {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("group_concat(")
			for _, expr := range ag.GroupExpr {
				buf.WriteString(fmt.Sprintf("%v ", expr))
			}
			buf.WriteString(")")
		}
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) error {
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
	if len(ap.Exprs) == 0 {
		ap.ctr.state = Build
	} else {
		ap.ctr.state = BuildWithGroup
	}
	if len(ap.ctr.aggVecs) == 0 {
		ap.ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	}
	if len(ap.ctr.multiVecs) == 0 {
		ap.ctr.multiVecs = make([][]evalVector, len(ap.MultiAggs))
		for i, agg := range ap.MultiAggs {
			ap.ctr.multiVecs[i] = make([]evalVector, len(agg.GroupExpr))
		}
	}
	if len(ap.ctr.groupVecs) == 0 {
		ap.ctr.vecs = make([]*vector.Vector, len(ap.Exprs))
		ap.ctr.groupVecs = make([]evalVector, len(ap.Exprs))
	}
	ap.ctr.pms[ap.ctr.pmIdx].Bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	for {
		switch ctr.state {
		case Build:
			bat := proc.InputBatch()
			if bat == nil {
				ctr.state = Eval
				continue
			}
			if len(bat.Zs) == 0 {
				return false, nil
			}
			err := ctr.build(ap, bat, proc, anal)
			return false, err
		case BuildWithGroup:
			bat := proc.InputBatch()
			if bat == nil {
				ctr.state = EvalWithGroup
				continue
			}
			if len(bat.Zs) == 0 {
				return false, nil
			}
			err := ctr.buildWithGroup(ap, bat, proc, anal)
			return false, err
		case Eval:
			ctr.state = End
			if ctr.isEmpty {
				b := batch.NewWithSize(len(ap.Types))
				for i := range b.Vecs {
					b.Vecs[i] = vector.New(ap.Types[i])
				}
				if err := ctr.build(ap, b, proc, anal); err != nil {
					ctr.state = End
					return false, err
				}
			}
			anal.Alloc(int64(ctr.pms[0].Bat.Size()))
			anal.Output(ctr.pms[0].Bat, isLast)
			proc.SetInputBatch(ctr.pms[0].Bat)
			return true, nil
		case EvalWithGroup:
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

func (ctr *container) build(ap *Argument, bat *batch.Batch, proc *process.Process, anal process.Analyze) error {
	defer func() { ctr.isEmpty = false }()
	if bat.VectorCount() == 0 {
		return nil
	}
	proc.SetInputBatch(&batch.Batch{})
	if err := ctr.evalAggVector(bat, ap.Aggs, proc, anal); err != nil {
		return err
	}
	defer ctr.cleanAggVectors(proc.Mp())
	if err := ctr.evalMultiAggs(bat, ap.MultiAggs, proc, anal); err != nil {
		return err
	}
	defer ctr.cleanMultiAggVecs(proc.Mp())
	if ctr.isEmpty {
		if err := ap.initAggInfo(proc); err != nil {
			return err
		}
		ap.ctr.pms[ap.ctr.pmIdx].Bat.Zs = append(ap.ctr.pms[ap.ctr.pmIdx].Bat.Zs, 0)
		for _, ag := range ap.ctr.pms[ap.ctr.pmIdx].Bat.Aggs {
			if err := ag.Grows(1, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return ctr.processH0(bat, ap, proc)
}

func (ctr *container) buildWithGroup(ap *Argument, bat *batch.Batch, proc *process.Process, anal process.Analyze) error {
	var err error

	proc.SetInputBatch(&batch.Batch{})
	if err = ctr.evalAggVector(bat, ap.Aggs, proc, anal); err != nil {
		return err
	}
	defer ctr.cleanAggVectors(proc.Mp())
	if err = ctr.evalMultiAggs(bat, ap.MultiAggs, proc, anal); err != nil {
		return err
	}
	defer ctr.cleanMultiAggVecs(proc.Mp())
	if err = ctr.evalGroupVector(bat, ap.Exprs, proc, anal); err != nil {
		return err
	}
	defer ctr.cleanGroupVectors(proc.Mp())
	if ctr.isEmpty {
		size := 0
		ctr.isEmpty = false
		for i := range ctr.groupVecs {
			switch ctr.groupVecs[i].vec.GetType().TypeSize() {
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
		switch {
		case size <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(true, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(true, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return err
			}
		}
		if err := ap.initAggInfo(proc); err != nil {
			return err
		}
	}
	if rows := ctr.pms[ctr.pmIdx].Bat.Length(); rows > defines.DefaultVectorRows {
		pm := new(colexec.PrivMem)
		ctr.pmIdx++
		pm.InitByTypes(ap.Types, proc)
		ctr.pms = append(ctr.pms, pm)
		pm.Bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
		if err := ap.initAggInfo(proc); err != nil {
			return err
		}
	}
	if ctr.typ == H8 {
		return ctr.processH8(bat, proc)
	}
	return ctr.processHStr(bat, proc)
}

func (ctr *container) processH0(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	for _, z := range bat.Zs {
		ctr.pms[ctr.pmIdx].Bat.Zs[0] += z
	}
	for i, ag := range ctr.pms[ctr.pmIdx].Bat.Aggs {
		if i < len(ctr.aggVecs) {
			if err := ag.BulkFill(0, bat.Zs, []*vector.Vector{ctr.aggVecs[i].vec}); err != nil {
				return err
			}
		} else {
			if err := ag.BulkFill(0, bat.Zs, ctr.ToVecotrs(i-len(ctr.aggVecs))); err != nil {
				return err
			}
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
		vals, _, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		if err := ctr.batchFill(i, n, bat, vals, rows, proc); err != nil {
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
		rows := ctr.strHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		if err := ctr.batchFill(i, n, bat, vals, rows, proc); err != nil {
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
	for k, v := range vals[:n] {
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
			srcVec := ctr.groupVecs[j].vec
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
			if j < len(ctr.aggVecs) {
				err := ag.BatchFill(int64(i), ctr.chunkInserted, ctr.chunkValues, bat.Zs, []*vector.Vector{ctr.aggVecs[j].vec})
				if err != nil {
					return err
				}
			} else {
				err := ag.BatchFill(int64(i), ctr.chunkInserted, ctr.chunkValues, bat.Zs, ctr.ToVecotrs(j-len(ctr.aggVecs)))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *container) evalAggVector(bat *batch.Batch, aggs []agg.Aggregate, proc *process.Process, analyze process.Analyze) error {
	for i, ag := range aggs {
		vec, err := colexec.EvalExpr(bat, proc, ag.E)
		if err != nil {
			ctr.cleanAggVectors(proc.Mp())
			return err
		}
		ctr.aggVecs[i].vec = vec
		ctr.aggVecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.aggVecs[i].needFree = false
				break
			}
		}
		if ctr.aggVecs[i].needFree && vec != nil {
			analyze.Alloc(int64(vec.Size()))
		}
	}
	return nil
}

func (ctr *container) evalGroupVector(bat *batch.Batch, exprs []*plan.Expr, proc *process.Process, anal process.Analyze) error {
	for i, expr := range exprs {
		vec, err := colexec.EvalExpr(bat, proc, expr)
		if err != nil {
			ctr.cleanGroupVectors(proc.Mp())
			return err
		}
		ctr.groupVecs[i].vec = vec
		ctr.groupVecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.groupVecs[i].needFree = false
				break
			}
		}
		if ctr.groupVecs[i].needFree && vec != nil {
			anal.Alloc(int64(vec.Size()))
		}
		ctr.vecs[i] = vec
	}
	return nil
}

func (ctr *container) evalMultiAggs(bat *batch.Batch, multiAggs []group_concat.Argument, proc *process.Process, analyze process.Analyze) error {
	for i := range multiAggs {
		for j, expr := range multiAggs[i].GroupExpr {
			vec, err := colexec.EvalExpr(bat, proc, expr)
			if err != nil {
				ctr.cleanMultiAggVecs(proc.Mp())
				return err
			}
			ctr.multiVecs[i][j].vec = vec
			ctr.multiVecs[i][j].needFree = true
			for k := range bat.Vecs {
				if bat.Vecs[k] == vec {
					ctr.multiVecs[i][j].needFree = false
					break
				}
			}
			if ctr.multiVecs[i][j].needFree && vec != nil {
				analyze.Alloc(int64(vec.Size()))
			}
		}
	}
	return nil
}

func (ap *Argument) initAggInfo(proc *process.Process) error {
	var err error

	for i, ag := range ap.Aggs {
		if ap.ctr.pms[ap.ctr.pmIdx].Bat.Aggs[i], err = agg.New(ag.Op, ag.Dist,
			ap.ctr.aggVecs[i].vec.Typ); err != nil {
			return err
		}
	}
	for i, agg := range ap.MultiAggs {
		if ap.ctr.pms[ap.ctr.pmIdx].Bat.Aggs[i+len(ap.Aggs)] = group_concat.NewGroupConcat(&agg,
			ap.ctr.ToInputType(i)); err != nil {
			return err
		}
	}
	return nil
}
