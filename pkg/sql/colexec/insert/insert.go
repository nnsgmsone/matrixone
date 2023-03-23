// Copyright 2021 Matrix Origin
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

package insert

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.IsRemote {
		ap.Container = colexec.NewWriteS3Container(ap.InsertCtx.TableDef)
	}
	ap.ctr = new(container)
	return ap.ctr.InitByTypes(ap.Types, proc)
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	t := time.Now()
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer func() {
		anal.Stop()
		anal.AddInsertTime(t)
	}()
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		if ap.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			if err := ap.Container.WriteS3CacheBatch(proc); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	anal.Input(bat, isFirst)
	affectedRows := bat.Length()
	if err := ap.InsertCtx.Source.Write(proc.Ctx, bat); err != nil {
		return false, err
	}
	//insertCtx := ap.InsertCtx
	/*
		affectedRows, err := colexec.InsertBatch(ap.Container, proc, bat, insertCtx.Source,
			insertCtx.TableDef, insertCtx.UniqueSource)
		if err != nil {
			return false, err
		}
	*/
	if ap.IsRemote {
		ap.Container.WriteEnd(proc)
	}
	atomic.AddUint64(&ap.Affected, affectedRows)
	return false, nil
}
