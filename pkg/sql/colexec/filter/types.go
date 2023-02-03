package filter

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/expr"
)

type Filter struct {
	e           expr.Expr[any]
	bat         *batch.Batch
	outputTypes []types.Type
	ifs         []func([]int64)
}
