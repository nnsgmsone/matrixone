// Copyright 2022 Matrix Origin
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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/timediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TimeDiff[T timediff.DiffT](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//input vectors
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[T](firstVector)
	secondValues := vector.MustTCols[T](secondVector)
	resultType := types.T_time.ToType()

	resultPrecision := firstVector.GetType().Precision
	if firstVector.GetType().Precision < secondVector.GetType().Precision {
		resultPrecision = secondVector.GetType().Precision
	}
	resultType.Precision = resultPrecision

	if firstVector.IsConstNull() || secondVector.IsConstNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	vectorLen := len(firstValues)
	if firstVector.IsConst() {
		vectorLen = len(secondValues)
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, int64(vectorLen), nil)
	if err != nil {
		return nil, err
	}

	rs := vector.MustTCols[types.Time](resultVector)
	nulls.Or(firstVector.Nsp, secondVector.Nsp, resultVector.Nsp)
	if err = timediff.TimeDiffWithTimeFn(firstValues, secondValues, rs); err != nil {
		return nil, err
	}
	return resultVector, nil
}
