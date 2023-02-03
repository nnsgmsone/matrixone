package filter

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (op *Filter) Prepare(proc *process.Process) error {
	op.bat = batch.New(len(op.outputTypes))
	op.ifs = make([]func([]int64), len(op.outputTypes))
	for i, ot := range op.outputTypes {
		vec := vector.New(vector.FLAT, ot)
		op.bat.SetVector(i, vec)
		op.ifs[i] = vec.GetShrinkFunction()
	}
	return nil
}

func (op *Filter) Exec(proc *process.Process) (bool, error) {
	bat := proc.GetInputBatch()
	if bat == nil {
		return true, nil
	}
	defer bat.Relinquish()
	if bat.Length() == 0 {
		return false, nil
	}
	vec, err := op.e.EvalForSelect(bat, proc)
	if err != nil {
		return false, nil
	}
	op.bat.WaitForAllAbstain()
	for i := 0; i < bat.VectorCount(); i++ {
		op.bat.GetVector(i).Reset()
	}
	sels := vector.GetColumnValue[int64](vec)
	for i := 0; i < bat.VectorCount(); i++ {
		op.ifs[i](sels)
	}
	op.bat.DeliveredToConsumers(1)
	proc.SetOutputBatch(op.bat)
	return false, nil
}

func (op *Filter) Free(proc *process.Process) {
	op.e.Free(proc)
	if op.bat != nil {
		op.bat.Free(proc.GetMPool())
	}
}
