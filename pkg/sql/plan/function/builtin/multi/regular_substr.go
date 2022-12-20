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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularSubstr(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	resultType := types.T_varchar.ToType()

	//maxLen
	maxLen := vectors[0].Length()
	for i := range vectors {
		val := vectors[i].Length()
		if val > maxLen {
			maxLen = val
		}
	}

	//optional arguments
	var pos []int64
	var occ []int64
	var match_type []string

	//different parameter length conditions
	switch len(vectors) {
	case 2:
		pos = []int64{1}
		occ = []int64{1}
		match_type = []string{"c"}

	case 3:
		pos = vector.MustTCols[int64](vectors[2])
		occ = []int64{1}
		match_type = []string{"c"}
	case 4:
		pos = vector.MustTCols[int64](vectors[2])
		occ = vector.MustTCols[int64](vectors[3])
		match_type = []string{"c"}
	}

	if firstVector.IsConstNull() || secondVector.IsConstNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, 0, nil)
	if err != nil {
		return nil, err
	}
	err = regular.RegularSubstrWithArrays(firstValues, secondValues, pos, occ, match_type, firstVector.GetNulls(), secondVector.GetNulls(), resultVector, proc, maxLen)
	if err != nil {
		return nil, err
	}
	return resultVector, nil
}
