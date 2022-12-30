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
	"github.com/matrixorigin/matrixone/pkg/vectorize/date_sub"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[types.Date](vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])

	resultType := types.Type{Oid: types.T_date, Size: 4}
	if firstVector.IsConst() && secondVector.IsConst() {
		if firstVector.IsConstNull() || secondVector.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Date, 1)
		res, err := date_sub.DateSub(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		_, err = date_sub.DateSub(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), resultValues)
		return resultVector, err
	}
}

func DatetimeSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[types.Datetime](vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])

	precision := firstVector.GetType().Precision
	switch types.IntervalType(thirdValues[0]) {
	case types.MicroSecond:
		precision = 6
	}

	resultType := types.Type{Oid: types.T_datetime, Precision: precision, Size: 8}
	if firstVector.IsConst() && secondVector.IsConst() {
		if firstVector.IsConstNull() || secondVector.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Datetime, 1)
		res, err := date_sub.DatetimeSub(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Datetime](resultVector)
		_, err = date_sub.DatetimeSub(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), resultValues)
		return resultVector, err
	}
}

func DateStringSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustStrCols(vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])
	resultType := types.Type{Oid: types.T_datetime, Precision: 6, Size: 8}

	if firstVector.IsConst() && secondVector.IsConst() {
		if firstVector.IsConstNull() || secondVector.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Datetime, 1)
		res, err := date_sub.DateStringSub(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Datetime](resultVector)
		_, err = date_sub.DateStringSub(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), resultValues)
		return resultVector, err
	}
}

func TimeStampSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[types.Timestamp](vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])

	precision := firstVector.GetType().Precision
	switch types.IntervalType(thirdValues[0]) {
	case types.MicroSecond:
		precision = 6
	}

	resultType := types.Type{Oid: types.T_timestamp, Precision: precision, Size: 8}
	if firstVector.IsConst() && secondVector.IsConst() {
		if firstVector.IsConstNull() || secondVector.IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Timestamp, 1)
		res, err := date_sub.TimestampSub(proc.SessionInfo.TimeZone, firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), firstVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Timestamp](resultVector)
		_, err = date_sub.TimestampSub(proc.SessionInfo.TimeZone, firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), resultVector.GetNulls(), resultValues)
		return resultVector, err
	}
}
