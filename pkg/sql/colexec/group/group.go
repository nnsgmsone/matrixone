package group

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (op *Group) Prepare(proc *process.Process) error {
	op.bat = batch.New(len(op.outputTypes))
	for i, ot := range op.outputTypes {
		op.bat.SetVector(i, vector.New(vector.FLAT, ot))
	}
	op.aggVecs = make([]*vector.Vector, len(op.aggExprs))
	op.groupVecs = make([]*vector.Vector, len(op.groupVecs))
	return nil
}

func (op *Group) Call(proc *process.Process) (bool, error) {
	bat := proc.GetInputBatch()
	if bat == nil {
		return true, nil
	}
	defer bat.Relinquish()
	if bat.Length() == 0 {
		return false, nil
	}
	for i, e := range op.aggExprs {
		vec, err := e.Eval(bat, proc)
		if err != nil {
			return false, err
		}
		op.aggVecs[i] = vec
	}
	for i, e := range op.groupExprs {
		vec, err := e.Eval(bat, proc)
		if err != nil {
			return false, err
		}
		op.groupVecs[i] = vec
	}
	//process()
	proc.SetOutputBatch(op.bat)
	return false, nil
}

func (op *Group) Free(proc *process.Process) {
	for _, e := range op.aggExprs {
		e.Free(proc)
	}
	for _, e := range op.groupExprs {
		e.Free(proc)
	}
	if op.bat != nil {
		op.bat.Free(proc.GetMPool())
	}
}
