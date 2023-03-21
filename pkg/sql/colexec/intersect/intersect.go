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

package intersect

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" intersect ")
}

func Prepare(proc *process.Process, arg any) error {
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.hashTable, err = hashmap.NewStrMap(true, ap.IBucket, ap.NBucket, proc.Mp())
	if err != nil {
		return err
	}
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	return ap.ctr.InitByTypes(ap.Types, proc)
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()
	ap := arg.(*Argument)
	for {
		switch ap.ctr.state {
		case Build:
			if err := ap.ctr.buildHashTable(proc, analyze, isFirst); err != nil {
				ap.ctr.state = End
				return false, err
			}
			if ap.ctr.hashTable != nil {
				analyze.Alloc(ap.ctr.hashTable.Size())
			}
			ap.ctr.state = Probe
		case Probe:
			var err error

			isLast := false
			if isLast, err = ap.ctr.probeHashTable(proc, analyze, isFirst, isLast); err != nil {
				ap.ctr.state = End
				return true, err
			}
			if isLast {
				ap.ctr.state = End
				continue
			}
			return false, nil
		case End:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

// build hash table
func (c *container) buildHashTable(proc *process.Process, anal process.Analyze, isFirst bool) error {
	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[1].Ch
		anal.WaitStop(start)
		// last batch of block
		if bat == nil {
			break
		}
		// empty batch
		if bat.Length() == 0 {
			bat.SubCnt(1)
			continue
		}
		anal.Input(bat, isFirst)
		cnt := bat.Length()
		itr := c.hashTable.NewIterator()
		for i := 0; i < cnt; i += hashmap.UnitLimit {
			rowcnt := c.hashTable.GroupCount()
			n := cnt - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vs, zs, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.SubCnt(1)
				return err
			}
			for j, v := range vs {
				if zs[j] == 0 {
					continue
				}
				if v > rowcnt {
					c.cnts = append(c.cnts, proc.Mp().GetSels())
					c.cnts[v-1] = append(c.cnts[v-1], 1)
					rowcnt++
				}
			}
		}
		bat.SubCnt(1)
	}
	return nil
}

func (ctr *container) probeHashTable(proc *process.Process, anal process.Analyze,
	isFirst bool, isLast bool) (bool, error) {
	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		anal.WaitStop(start)
		// last batch of block
		if bat == nil {
			return true, nil
		}
		// empty batch
		if bat.Length() == 0 {
			bat.SubCnt(1)
			continue
		}
		anal.Input(bat, isFirst)
		needInsert := make([]uint8, hashmap.UnitLimit)
		resetsNeedInsert := make([]uint8, hashmap.UnitLimit)
		ctr.OutBat.Reset()
		cnt := bat.Length()
		itr := ctr.hashTable.NewIterator()
		for i := 0; i < cnt; i += hashmap.UnitLimit {
			n := cnt - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			copy(ctr.inBuckets, hashmap.OneUInt8s)
			copy(needInsert, resetsNeedInsert)
			insertcnt := 0
			vs, zs := itr.Find(i, n, bat.Vecs, ctr.inBuckets)
			for j, v := range vs {
				// not in the processed bucket
				if ctr.inBuckets[j] == 0 {
					continue
				}
				// null value
				if zs[j] == 0 {
					continue
				}
				// not found
				if v == 0 {
					continue
				}
				// has been added into output batch
				if ctr.cnts[v-1][0] == 0 {
					continue
				}
				needInsert[j] = 1
				ctr.cnts[v-1][0] = 0
				ctr.OutBat.Zs = append(ctr.OutBat.Zs, 1)
				insertcnt++
			}

			if insertcnt > 0 {
				for pos := range bat.Vecs {
					uf := ctr.Ufs[pos]
					for j := 0; j < n; j++ {
						if needInsert[j] == 1 {
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
