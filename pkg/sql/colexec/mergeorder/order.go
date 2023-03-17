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

package mergeorder

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("mergeorder([")
	for i, f := range ap.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.poses = make([]int32, 0, len(ap.Fs))
	ap.ctr.compare0Index = make([]int32, len(ap.Fs))
	ap.ctr.compare1Index = make([]int32, len(ap.Fs))
	ap.ctr.childrenCount = ap.ChildrenNumber
	ap.ctr.InitByTypes(ap.Types, proc)
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
			if err := ctr.build(ap, proc, anal, isFirst); err != nil {
				ctr.state = End
				return false, err
			}
			ctr.state = Eval
		case Eval:
			ctr.state = End
			for i := ctr.n; i < len(ctr.OutVecs); i++ {
				ctr.OutVecs[i].Free(proc.Mp())
			}
			ctr.OutBat.Vecs = ctr.OutBat.Vecs[:ctr.n]
			if err := ctr.OutBat.Shuffle(ctr.finalSelectList, proc.Mp()); err != nil {
				ctr.state = End
				return false, err
			}
			anal.Output(ctr.OutBat, isLast)
			proc.SetInputBatch(ctr.OutBat)
			return true, nil
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
		n := bat.VectorCount()
		if err := ctr.mergeSort(ap, bat, proc, anal); err != nil {
			for i := n; i < bat.VectorCount(); i++ { // free expr's memory
				bat.GetVector(int32(i)).Free(proc.Mp())
			}
			bat.Vecs = bat.Vecs[:n]
			bat.SubCnt(1)
			return err
		}
		for i := n; i < bat.VectorCount(); i++ { // free exprs' memory
			bat.GetVector(int32(i)).Free(proc.Mp())
		}
		bat.Vecs = bat.Vecs[:n]
		bat.SubCnt(1)
	}
}

func (ctr *container) mergeSort(ap *Argument, bat *batch.Batch,
	proc *process.Process, anal process.Analyze) error {
	ctr.n = bat.VectorCount()
	ctr.poses = ctr.poses[:0]

	// evaluate the order column.
	for _, f := range ap.Fs {
		vec, err := colexec.EvalExpr(bat, proc, f.Expr)
		if err != nil {
			return err
		}
		newColumn := true
		for i := range bat.Vecs {
			if bat.Vecs[i] == vec {
				newColumn = false
				ctr.poses = append(ctr.poses, int32(i))
				break
			}
		}
		if newColumn {
			ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
			bat.Vecs = append(bat.Vecs, vec)
			anal.Alloc(int64(vec.Size()))
		}
	}
	copy(ctr.compare1Index, ctr.poses)

	// init the compare structure if first time.
	if len(ctr.cmps) == 0 {
		var desc, nullsLast bool
		ctr.cmps = make([]compare.Compare, len(ap.Fs))
		for i := range ctr.cmps {
			desc = ap.Fs[i].Flag&plan.OrderBySpec_DESC != 0
			if ap.Fs[i].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
				nullsLast = false
			} else if ap.Fs[i].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
				nullsLast = true
			} else {
				nullsLast = desc
			}
			ctr.cmps[i] = compare.New(*bat.Vecs[ctr.poses[i]].GetType(), desc, nullsLast)
		}
	}
	return ctr.mergeSort2(bat, proc)
}

func (ctr *container) mergeSort2(bat2 *batch.Batch, proc *process.Process) error {
	if !ctr.init {
		ctr.init = true
		for i := ctr.n; i < bat2.VectorCount(); i++ {
			typ := *bat2.GetVector(int32(i)).GetType()
			ctr.Ufs = append(ctr.Ufs, vector.GetUnionOneFunction(typ, proc.Mp()))
			vec := vector.NewVec(typ)
			vec.PreExtend(defines.DefaultVectorRows, proc.Mp())
			ctr.OutVecs = append(ctr.OutVecs, vec)
			ctr.OutBat.Vecs = append(ctr.OutBat.Vecs, vec)
		}
		ctr.OutBat.Reset()
		for i, vec := range ctr.OutVecs {
			uf := ctr.Ufs[i]
			srcVec := bat2.GetVector(int32(i))
			for j := 0; j < bat2.Length(); j++ {
				if err := uf(vec, srcVec, int64(j)); err != nil {
					return err
				}
			}
		}
		ctr.OutBat.Zs = append(ctr.OutBat.Zs, bat2.Zs...)
		ctr.finalSelectList = generateSelectList(int64(ctr.OutBat.Length()))
		copy(ctr.compare0Index, ctr.poses)
		return nil
	}
	bat1 := ctr.OutBat
	// union bat1 and bat2
	// do merge sort, get order index list.
	s1, s2 := int64(0), int64(bat1.Vecs[0].Length()) // startIndexOfBat1, startIndexOfBat2

	for i, vec := range ctr.OutVecs {
		uf := ctr.Ufs[i]
		srcVec := bat2.GetVector(int32(i))
		for j := 0; j < bat2.Length(); j++ {
			if err := uf(vec, srcVec, int64(j)); err != nil {
				return err
			}
		}
	}
	bat1.Zs = append(bat1.Zs, bat2.Zs...)

	end1, end2 := s2, int64(bat1.Vecs[0].Length())
	sels := make([]int64, 0, end2)

	// set up cmp must happen after vector.UnionBatch.  UnionBatch may grow the vector
	// in bat1, which could cause a realloc.  Depending on mpool has fixed pool, the old
	// vector maybe destroyed, cmp then set a garbage vector.
	for i, cmp := range ctr.cmps {
		cmp.Set(0, bat1.GetVector(ctr.compare0Index[i]))
		cmp.Set(1, bat2.GetVector(ctr.compare1Index[i]))
	}

	for s1 < end1 && s2 < end2 {
		i := s1
		j := s2 - end1
		compareResult := 0
		for k := range ctr.cmps {
			compareResult = ctr.cmps[k].Compare(0, 1, ctr.finalSelectList[i], j)
			if compareResult != 0 {
				break
			}
		}
		if compareResult <= 0 {
			// weight of item1 is less or equal to item2
			sels = append(sels, ctr.finalSelectList[s1])
			s1++
		} else {
			sels = append(sels, s2)
			s2++
		}
	}
	for s1 < end1 {
		sels = append(sels, ctr.finalSelectList[s1])
		s1++
	}
	for s2 < end2 {
		sels = append(sels, s2)
		s2++
	}
	ctr.finalSelectList = sels
	return nil
}

func generateSelectList(j int64) []int64 {
	list := make([]int64, j)
	var i int64
	for i = 0; i < j; i++ {
		list[i] = i
	}
	return list
}

func makeFlagsOne(n int) []uint8 {
	t := make([]uint8, n)
	for i := range t {
		t[i]++
	}
	return t
}
