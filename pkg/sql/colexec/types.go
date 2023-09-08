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

package colexec

import (
	"reflect"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResultPos struct {
	Rel int32
	Pos int32
}

func NewResultPos(rel int32, pos int32) ResultPos {
	return ResultPos{Rel: rel, Pos: pos}
}

// ReceiveInfo used to spec which node,
// and which registers you need
type ReceiveInfo struct {
	// it's useless
	NodeAddr string
	Uuid     uuid.UUID
}

// TODO: remove batchCntMap when dispatch executor using the stream correctly
// Server used to support cn2s3 directly, for more info, refer to docs about it
type Server struct {
	sync.Mutex
	id uint64
	mp map[uint64]*process.WaitRegister

	hakeeper      logservice.CNHAKeeperClient
	CNSegmentId   types.Uuid
	InitSegmentId bool
	// currentFileOffset uint16
	uuidCsChanMap UuidProcMap
	//txn's local segments.
	cnSegmentMap CnSegmentMap
}

type uuidProcMapItem struct {
	proc *process.Process

	// if referenceCount is 0, it means all process dependent on this uuidProcMapItem is over.
	// and we can delete this item from uuidCsChanMap.
	referenceCount int
}

type UuidProcMap struct {
	sync.Mutex
	mp map[uuid.UUID]uuidProcMapItem
}

type CnSegmentMap struct {
	sync.Mutex
	// tag whether a segment is generated by this txn
	// segmentName => uuid + file number
	// 1.mp[segmentName] = 1 => txnWorkSpace
	// 2.mp[segmentName] = 2 => Cn Blcok
	mp map[objectio.Segmentid]int32
}

// ReceiverOperator need to receive batch from proc.Reg.MergeReceivers
type ReceiverOperator struct {
	proc *process.Process

	// parameter for Merge-Type receiver.
	// Merge-Type specifys the operator receive batch from all
	// regs or single reg.
	//
	// Merge/MergeGroup/MergeLimit ... are Merge-Type
	// while Join/Intersect/Minus ... are not
	aliveMergeReceiver int
	chs                []chan *batch.Batch
	receiverListener   []reflect.SelectCase
	producers          []string
}

type RuntimeFilterChan struct {
	Spec *plan.RuntimeFilterSpec
	Chan chan *pipeline.RuntimeFilter
}
