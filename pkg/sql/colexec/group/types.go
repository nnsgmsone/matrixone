package group

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/expr"
)

type Group struct {
	aggExprs    []expr.Expr[any]
	groupExprs  []expr.Expr[any]
	bat         *batch.Batch
	outputTypes []types.Type
	aggVecs     []*vector.Vector
	groupVecs   []*vector.Vector
}
