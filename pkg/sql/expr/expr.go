package expr

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (e *FuncExpr) Prepare(proc *process.Process) {
	e.vecs = make([]*vector.Vector, len(e.args))
	e.vec = vector.New(vector.FLAT, e.typ)
}

func (e *ConstExpr) Prepare(proc *process.Process) {
}

func (e *ColumnExpr) Prepare(proc *process.Process) {
}

func (e *FuncExpr) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, error) {
	var err error

	for i, arg := range e.args {
		if e.vecs[i], err = arg.Eval(bat, proc); err != nil {
			return nil, err
		}
	}
	return e.fn(e.vec, e.vecs, proc, bat.Length())
}

func (e *ConstExpr) Eval(_ *batch.Batch, _ *process.Process) (*vector.Vector, error) {
	return e.vec, nil
}

func (e *ColumnExpr) Eval(bat *batch.Batch, _ *process.Process) (*vector.Vector, error) {
	return bat.GetVector(e.pos), nil
}

func (e *FuncExpr) EvalForSelect(bat *batch.Batch, proc *process.Process) (*vector.Vector, error) {
	return nil, nil
}

func (e *ConstExpr) EvalForSelect(_ *batch.Batch, _ *process.Process) (*vector.Vector, error) {
	return e.vec, nil
}

func (e *ColumnExpr) EvalForSelect(bat *batch.Batch, _ *process.Process) (*vector.Vector, error) {
	return bat.GetVector(e.pos), nil
}
