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

package minus

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" minus ")
}

func Prepare(proc *process.Process, arg any) error {
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.hashTable, err = hashmap.NewStrMap(true, ap.IBucket, ap.NBucket, proc.Mp())
	if err != nil {
		return err
	}
	return ap.ctr.InitByTypes(ap.Types, proc)
}

// Call is the execute method of minus operator
// it built a hash table for right relation first.
// use values from left relation to probe and update the hash table.
// and preserve values that do not exist in the hash table.
func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	var err error

	// prepare the analysis work.
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	for {
		switch ap.ctr.state {
		case Build:
			// step 1: build the hash table by all right batches.
			if err = ap.ctr.buildHashTable(proc, anal, isFirst); err != nil {
				ap.ctr.state = End
				return false, err
			}
			if ap.ctr.hashTable != nil {
				anal.Alloc(ap.ctr.hashTable.Size())
			}
			ap.ctr.state = Probe
		case Probe:
			// step 2: use left batches to probe and update the hash table.
			//
			// only one batch is processed during each loop, and the batch will be sent to
			// next operator immediately after successful processing.
			last := false
			last, err = ap.ctr.probeHashTable(proc, anal, isFirst, isLast)
			if err != nil {
				ap.ctr.state = End
				return false, err
			}
			if last {
				ap.ctr.state = End
				continue
			}

		case End:
			// operator over.
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

// buildHashTable use all batches from proc.Reg.MergeReceiver[1] to build the hash map.
func (ctr *container) buildHashTable(proc *process.Process, anal process.Analyze, isFirst bool) error {
	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[1].Ch
		anal.WaitStop(start)
		// the last batch of pipeline.
		if bat == nil {
			break
		}
		// just an empty batch.
		if len(bat.Zs) == 0 {
			bat.SubCnt(1)
			continue
		}
		anal.Input(bat, isFirst)
		itr := ctr.hashTable.NewIterator()
		count := bat.Vecs[0].Length()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			_, _, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.SubCnt(1)
				return err
			}
		}
		bat.SubCnt(1)
	}
	return nil
}

// probeHashTable use a batch from proc.Reg.MergeReceivers[index] to probe and update the hash map.
// If a row of data never appears in the hash table, add it into hath table and send it to the next operator.
// if batch is the last one, return true, else return false.
func (ctr *container) probeHashTable(proc *process.Process, anal process.Analyze,
	isFirst bool, isLast bool) (bool, error) {
	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)
	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		anal.WaitStop(start)
		// the last batch of block.
		if bat == nil {
			return true, nil
		}
		// just an empty batch.
		if len(bat.Zs) == 0 {
			bat.SubCnt(1)
			continue
		}
		anal.Input(bat, isFirst)
		ctr.OutBat.Reset()
		count := bat.Length()
		itr := ctr.hashTable.NewIterator()
		for i := 0; i < count; i += hashmap.UnitLimit {
			oldHashGroup := ctr.hashTable.GroupCount()
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vs, _, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.SubCnt(1)
				return false, err
			}
			copy(inserted[:n], restoreInserted[:n])
			rows := oldHashGroup
			for j, v := range vs {
				if v > rows {
					// ensure that the same value will only be inserted once.
					rows++
					inserted[j] = 1
					ctr.OutBat.Zs = append(ctr.OutBat.Zs, 1)
				}
			}
			newHashGroup := ctr.hashTable.GroupCount()
			insertCount := int(newHashGroup - oldHashGroup)
			if insertCount > 0 {
				for pos := range bat.Vecs {
					uf := ctr.Ufs[pos]
					for j := 0; j < n; j++ {
						if inserted[j] == 1 {
							if err := uf(ctr.OutBat.Vecs[pos], bat.Vecs[pos], int64(j)); err != nil {
								bat.SubCnt(1)
								return false, err
							}
						}
					}
				}
			}
		}
		bat.SubCnt(1)
		anal.Output(ctr.OutBat, isLast)
		proc.SetInputBatch(ctr.OutBat)
		return false, nil
	}
}
