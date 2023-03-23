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

package table_function

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func genFilterMap(filters []string) map[string]struct{} {
	if filters == nil {
		return defaultFilterMap
	}
	filterMap := make(map[string]struct{}, len(filters))
	for _, f := range filters {
		filterMap[f] = struct{}{}
	}
	return filterMap
}

func unnestString(arg any, buf *bytes.Buffer) {
	buf.WriteString("unnest")
}

func unnestPrepare(proc *process.Process, arg *Argument) error {
	param := unnestParam{}
	param.ColName = string(arg.Params)
	if len(param.ColName) == 0 {
		param.ColName = "UNNEST_DEFAULT"
	}
	var filters []string
	for i := range arg.Attrs {
		denied := false
		for j := range unnestDeniedFilters {
			if arg.Attrs[i] == unnestDeniedFilters[j] {
				denied = true
				break
			}
		}
		if !denied {
			filters = append(filters, arg.Attrs[i])
		}
	}
	param.FilterMap = genFilterMap(filters)
	if len(arg.Args) < 1 || len(arg.Args) > 3 {
		return moerr.NewInvalidInput(proc.Ctx, "unnest: argument number must be 1, 2 or 3")
	}
	if len(arg.Args) == 1 {
		vType := types.T_varchar.ToType()
		bType := types.T_bool.ToType()
		arg.Args = append(arg.Args, &plan.Expr{Typ: plan2.MakePlan2Type(&vType), Expr: &plan.Expr_C{C: &plan2.Const{Value: &plan.Const_Sval{Sval: "$"}}}})
		arg.Args = append(arg.Args, &plan.Expr{Typ: plan2.MakePlan2Type(&bType), Expr: &plan.Expr_C{C: &plan2.Const{Value: &plan.Const_Bval{Bval: false}}}})
	} else if len(arg.Args) == 2 {
		bType := types.T_bool.ToType()
		arg.Args = append(arg.Args, &plan.Expr{Typ: plan2.MakePlan2Type(&bType), Expr: &plan.Expr_C{C: &plan2.Const{Value: &plan.Const_Bval{Bval: false}}}})
	}
	dt, err := json.Marshal(param)
	if err != nil {
		return err
	}
	arg.Params = dt
	return nil
}

func unnestCall(_ int, proc *process.Process, ap *Argument) (bool, error) {
	var (
		err      error
		jsonVec  *vector.Vector
		pathVec  *vector.Vector
		outerVec *vector.Vector
		path     bytejson.Path
		outer    bool
	)
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	jsonVec, err = colexec.EvalExpr(bat, proc, ap.Args[0])
	if err != nil {
		return false, err
	}
	jsonNeedFree := true
	for i := range bat.Vecs {
		if bat.Vecs[i] == jsonVec {
			jsonNeedFree = false
		}
	}
	defer func() {
		if jsonNeedFree {
			jsonVec.Free(proc.Mp())
		}
	}()
	if jsonVec.GetType().Oid != types.T_json && jsonVec.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("unnest: first argument must be json or string, but got %s", jsonVec.GetType().String()))
	}
	pathVec, err = colexec.EvalExpr(bat, proc, ap.Args[1])
	if err != nil {
		return false, err
	}
	pathNeedFree := true
	for i := range bat.Vecs {
		if bat.Vecs[i] == pathVec {
			pathNeedFree = false
		}
	}
	defer func() {
		if pathNeedFree {
			pathVec.Free(proc.Mp())
		}
	}()
	if pathVec.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("unnest: second argument must be string, but got %s", pathVec.GetType().String()))
	}
	outerVec, err = colexec.EvalExpr(bat, proc, ap.Args[2])
	if err != nil {
		return false, err
	}
	outerNeedFree := true
	for i := range bat.Vecs {
		if bat.Vecs[i] == outerVec {
			outerNeedFree = false
		}
	}
	defer func() {
		if outerNeedFree {
			outerVec.Free(proc.Mp())
		}
	}()
	if outerVec.GetType().Oid != types.T_bool {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("unnest: third argument must be bool, but got %s", outerVec.GetType().String()))
	}
	if !pathVec.IsConst() || !outerVec.IsConst() {
		return false, moerr.NewInvalidInput(proc.Ctx, "unnest: second and third arguments must be scalar")
	}
	path, err = types.ParseStringToPath(pathVec.GetStringAt(0))
	if err != nil {
		return false, err
	}
	outer = vector.MustFixedCol[bool](outerVec)[0]
	param := unnestParam{}
	if err = json.Unmarshal(ap.Params, &param); err != nil {
		return false, err
	}
	if len(ap.Types) == 0 {
		for i := range ap.Rets {
			ap.Types = append(ap.Types, dupType(ap.Rets[i].Typ))
		}
		ap.ctr.InitByTypes(ap.Types, proc)
	}
	switch jsonVec.GetType().Oid {
	case types.T_json:
		err = handle(jsonVec, &path, outer, &param, ap, proc, parseJson)
	case types.T_varchar:
		err = handle(jsonVec, &path, outer, &param, ap, proc, parseStr)
	}
	if err != nil {
		return false, err
	}
	proc.SetInputBatch(ap.ctr.OutBat)
	return false, nil
}

func handle(jsonVec *vector.Vector, path *bytejson.Path, outer bool,
	param *unnestParam, ap *Argument, proc *process.Process,
	fn func(dt []byte) (bytejson.ByteJson, error)) error {
	var (
		err  error
		json bytejson.ByteJson
		ures []bytejson.UnnestResult
	)

	ap.ctr.OutBat.SetAttributes(ap.Attrs)
	ap.ctr.OutBat.Reset()
	if jsonVec.IsConst() {
		json, err = fn(jsonVec.GetBytesAt(0))
		if err != nil {
			return err
		}
		ures, err = json.Unnest(path, outer, unnestRecursive, unnestMode, param.FilterMap)
		if err != nil {
			return err
		}
		if err = makeBatch(ures, param, ap, proc); err != nil {
			return err
		}
		return nil
	}
	jsonSlice := vector.ExpandBytesCol(jsonVec)
	rows := 0
	for i := range jsonSlice {
		json, err = fn(jsonSlice[i])
		if err != nil {
			return err
		}
		ures, err = json.Unnest(path, outer, unnestRecursive, unnestMode, param.FilterMap)
		if err != nil {
			return err
		}
		if err = makeBatch(ures, param, ap, proc); err != nil {
			return err
		}
		rows += len(ures)
	}
	for i := 0; i < rows; i++ {
		ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, int64(i))
	}
	return nil
}

func makeBatch(ures []bytejson.UnnestResult, param *unnestParam,
	ap *Argument, proc *process.Process) error {
	for i := 0; i < len(ures); i++ {
		for j := 0; j < len(ap.Attrs); j++ {
			vec := ap.ctr.OutBat.GetVector(int32(j))
			var err error
			switch ap.Attrs[j] {
			case "col":
				err = vector.AppendBytes(vec, []byte(param.ColName), false, proc.Mp())
			case "seq":
				err = vector.AppendFixed(vec, int32(i), false, proc.Mp())
			case "index":
				val, ok := ures[i][ap.Attrs[j]]
				if !ok || val == nil {
					err = vector.AppendFixed(vec, int32(0), true, proc.Mp())
				} else {
					intVal, _ := strconv.ParseInt(string(val), 10, 32)
					err = vector.AppendFixed(vec, int32(intVal), false, proc.Mp())
				}
			case "key", "path", "value", "this":
				val, ok := ures[i][ap.Attrs[j]]
				err = vector.AppendBytes(vec, val, !ok || val == nil, proc.Mp())
			default:
				err = moerr.NewInvalidArg(proc.Ctx, "unnest: invalid column name:%s", ap.Attrs[j])
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func parseJson(dt []byte) (bytejson.ByteJson, error) {
	ret := types.DecodeJson(dt)
	return ret, nil
}
func parseStr(dt []byte) (bytejson.ByteJson, error) {
	return types.ParseSliceToByteJson(dt)
}
