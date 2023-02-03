package expr

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Expr[T any] interface {
	Free(*process.Process)
	Prepare(*process.Process) error
	Eval(*batch.Batch, *process.Process) (*vector.Vector, error)
	// 想要校验select list和[]bool的差别
	EvalForSelect(*batch.Batch, *process.Process) (*vector.Vector, error)
}

// convert from plan.Expr
type FuncExpr struct {
	args []Expr[any]
	typ  types.Type
	vec  *vector.Vector
	vecs []*vector.Vector
	fn   func(*vector.Vector, []*vector.Vector, *process.Process, int) (*vector.Vector, error)
}

// convert from plan.Expr
type ConstExpr struct {
	vec *vector.Vector
}

// convert from plan.Expr
type ColumnExpr struct {
	pos int
}

func (e *FuncExpr) Free(proc *process.Process) {
	e.vec.Free(proc.GetMPool())
}

func (e *ConstExpr) Free(proc *process.Process) {
	e.vec.Free(proc.GetMPool())
}

func (e *ColumnExpr) Free(proc *process.Process) {
}
