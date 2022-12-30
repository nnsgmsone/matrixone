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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Like(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustStrCols(lv), vector.MustBytesCols(rv)
	rtyp := types.T_bool.ToType()

	if lv.IsConstNull() || rv.IsConstNull() {
		return proc.AllocConstNullVector(rtyp, lv.Length()), nil
	}

	var err error
	rs := make([]bool, lv.Length())

	switch {
	case !lv.IsConst() && rv.IsConst():
		if nulls.Any(lv.GetNulls()) {
			rs, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		return vector.NewWithFixed(rtyp, rs, lv.GetNulls(), proc.Mp()), nil
	case lv.IsConst() && rv.IsConst(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		return vector.New(vector.CONSTANT, rtyp), nil
	case lv.IsConst() && !rv.IsConst():
		rs, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.GetNulls(), rs)
		if err != nil {
			return nil, err
		}
		return vector.NewWithFixed(rtyp, rs, lv.GetNulls(), proc.Mp()), nil
	case !lv.IsConst() && !rv.IsConst():
		var nsp *nulls.Nulls
		if nulls.Any(rv.GetNulls()) && nulls.Any(lv.GetNulls()) {
			nsp = lv.GetNulls().Or(rv.GetNulls())
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if nulls.Any(rv.GetNulls()) && !nulls.Any(lv.GetNulls()) {
			nsp = rv.GetNulls()
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if !nulls.Any(rv.GetNulls()) && nulls.Any(lv.GetNulls()) {
			nsp = lv.GetNulls()
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndSlice(lvs, rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		return vector.NewWithFixed(rtyp, rs, nsp, proc.Mp()), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unexpected case for LIKE operator")
}
