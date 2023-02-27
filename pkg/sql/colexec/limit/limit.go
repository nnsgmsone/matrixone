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

package limit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var emptyBatch = &batch.Batch{}

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", ap.Limit))
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.bat = batch.NewWithSize(len(ap.Types))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Types))
	for i := range ap.Types {
		vec := vector.New(ap.Types[i])
		ap.ctr.vecs[i] = vec
		ap.ctr.bat.SetVector(int32(i), vec)
	}
	// init ufs
	return nil
}

// Call returning only the first n tuples from its input
func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)
	if ap.Seen >= ap.Limit {
		proc.Reg.InputBatch = nil
		bat.Clean(proc.Mp())
		return true, nil
	}
	length := bat.Length()
	newSeen := ap.Seen + uint64(length)
	if newSeen >= ap.Limit { // limit - seen
		batch.SetLength(bat, int(ap.Limit-ap.Seen))
		ap.Seen = newSeen
		anal.Output(bat, isLast)
		proc.SetInputBatch(bat)
		return true, nil
	}
	anal.Output(bat, isLast)
	ap.Seen = newSeen
	return false, nil
}
