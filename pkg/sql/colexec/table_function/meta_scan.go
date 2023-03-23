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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func metaScanPrepare(_ *process.Process, ap *Argument) error {
	return nil
}

func metaScanCall(_ int, proc *process.Process, ap *Argument) (bool, error) {
	var err error

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	v, err := colexec.EvalExpr(bat, proc, ap.Args[0])
	if err != nil {
		return false, err
	}
	needFree := true
	for i := range bat.Vecs {
		if bat.Vecs[i] == v {
			needFree = false
		}
	}
	defer func() {
		if needFree {
			v.Free(proc.Mp())
		}
	}()
	uuid := vector.MustFixedCol[types.Uuid](v)[0]
	// get file size
	path := catalog.BuildQueryResultMetaPath(proc.SessionInfo.Account, uuid.ToString())
	e, err := proc.FileService.StatFile(proc.Ctx, path)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return false, moerr.NewResultFileNotFound(proc.Ctx, path)
		}
		return false, err
	}
	// read meta's meta
	reader, err := blockio.NewFileReader(proc.FileService, path)
	if err != nil {
		return false, err
	}
	var idxs []uint16
	for i, name := range catalog.MetaColNames {
		for _, attr := range ap.Attrs {
			if name == attr {
				idxs = append(idxs, uint16(i))
			}
		}
	}
	// read meta's data
	bats, err := reader.LoadAllColumns(proc.Ctx, idxs, e.Size, common.DefaultAllocator)
	if err != nil {
		return false, err
	}
	if len(bats) == 0 {
		proc.SetInputBatch(&batch.Batch{})
	}
	if len(ap.Types) == 0 {
		for i := 0; i < bats[0].VectorCount(); i++ {
			ap.Types = append(ap.Types, *bats[0].GetVector(int32(i)).GetType())
		}
		ap.ctr.InitByTypes(ap.Types, proc)
	}
	ap.ctr.OutBat.SetAttributes(catalog.MetaColNames)
	ap.ctr.OutBat.Reset()
	for i, vec := range ap.ctr.OutVecs {
		uf := ap.ctr.Ufs[i]
		srcVec := bats[0].GetVector(int32(i))
		for j := 0; j < bats[0].Length(); j++ {
			if err := uf(vec, srcVec, int64(j)); err != nil {
				return false, err
			}
		}
	}
	ap.ctr.OutBat.Zs = append(ap.ctr.OutBat.Zs, 1)
	ap.ctr.OutBat.InitZsOne(1)
	proc.SetInputBatch(ap.ctr.OutBat)
	return false, nil
}
