// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func currentAccountPrepare(proc *process.Process, ap *Argument) error {
	if len(ap.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "current_account: no argument is required")
	}
	return nil
}

func getAccountName(proc *process.Process) *vector.Vector {
	return vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.SessionInfo.Account), 1, proc.Mp())
}

func getRoleName(proc *process.Process) *vector.Vector {
	return vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.SessionInfo.Role), 1, proc.Mp())
}

func getUserName(proc *process.Process) *vector.Vector {
	return vector.NewConstBytes(types.T_varchar.ToType(), []byte(proc.SessionInfo.User), 1, proc.Mp())
}

func getAccountId(proc *process.Process) *vector.Vector {
	return vector.NewConstFixed(types.T_uint32.ToType(), proc.SessionInfo.AccountId, 1, proc.Mp())
}

func getRoleId(proc *process.Process) *vector.Vector {
	return vector.NewConstFixed(types.T_uint32.ToType(), proc.SessionInfo.RoleId, 1, proc.Mp())
}

func getUserId(proc *process.Process) *vector.Vector {
	return vector.NewConstFixed(types.T_uint32.ToType(), proc.SessionInfo.UserId, 1, proc.Mp())
}

func currentAccountCall(_ int, proc *process.Process, ap *Argument) (bool, error) {
	var srcVec *vector.Vector

	if len(ap.Types) == 0 {
		for _, attr := range ap.Attrs {
			switch attr {
			case "account_name":
				ap.Types = append(ap.Types, types.T_varchar.ToType())
			case "account_id":
				ap.Types = append(ap.Types, types.T_uint32.ToType())
			case "user_name":
				ap.Types = append(ap.Types, types.T_varchar.ToType())
			case "user_id":
				ap.Types = append(ap.Types, types.T_uint32.ToType())
			case "role_name":
				ap.Types = append(ap.Types, types.T_varchar.ToType())
			case "role_id":
				ap.Types = append(ap.Types, types.T_uint32.ToType())
			default:
				return false, moerr.NewInvalidInput(proc.Ctx, "%v is not supported by current_account()", attr)
			}
		}
		ap.ctr.InitByTypes(ap.Types, proc)
	}
	ap.ctr.OutBat.SetAttributes(ap.Attrs)
	ap.ctr.OutBat.Reset()
	for i, vec := range ap.ctr.OutVecs {
		uf := ap.ctr.Ufs[i]
		switch ap.Attrs[i] {
		case "account_name":
			srcVec = getAccountName(proc)
		case "account_id":
			srcVec = getAccountId(proc)
		case "user_name":
			srcVec = getUserName(proc)
		case "user_id":
			srcVec = getUserId(proc)
		case "role_name":
			srcVec = getRoleName(proc)
		case "role_id":
			srcVec = getRoleId(proc)
		default:
			return false, moerr.NewInvalidInput(proc.Ctx, "%v is not supported by current_account()", ap.Attrs[i])
		}
		if err := uf(vec, srcVec, 0); err != nil {
			srcVec.Free(proc.Mp())
			return false, err
		}
		srcVec.Free(proc.Mp())
	}
	proc.SetInputBatch(ap.ctr.OutBat)
	return true, nil
}
