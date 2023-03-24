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

package dispatch

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type WrapperClientSession struct {
	msgId uint64
	cs    morpc.ClientSession
	uuid  uuid.UUID
	// toAddr string
	doneCh chan struct{}
}
type container struct {
	// the clientsession info for the channel you want to dispatch
	remoteReceivers []*WrapperClientSession
	// sendFunc is the rule you want to send batch
	sendFunc func(bat *batch.Batch, ap *Argument, proc *process.Process) error

	bat  *batch.Batch
	vecs []*vector.Vector
	ufs  []func(*vector.Vector, *vector.Vector, int64) error
}

type Argument struct {
	ctr      *container
	prepared bool
	sendCnt  int

	Types []types.Type // output vector types

	// FuncId means the sendFunc
	FuncId int
	// LocalRegs means the local register you need to send to.
	LocalRegs []*process.WaitRegister
	// RemoteRegs specific the remote reg you need to send to.
	RemoteRegs []colexec.ReceiveInfo
}

func (ap *Argument) ReturnTypes() []types.Type {
	return ap.Types
}

func (ap *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if ap.ctr.remoteReceivers != nil {
		for _, r := range ap.ctr.remoteReceivers {
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
			_ = cancel
			message := cnclient.AcquireMessage()
			{
				message.Id = r.msgId
				message.Cmd = pipeline.BatchMessage
				message.Sid = pipeline.MessageEnd
				message.Uuid = r.uuid[:]
			}
			if pipelineFailed {
				err := moerr.NewInternalErrorNoCtx("pipeline failed")
				message.Err = pipeline.EncodedMessageError(timeoutCtx, err)
			}
			r.cs.Write(timeoutCtx, message)
			close(r.doneCh)
		}

	}

	if pipelineFailed {
		for i := range ap.LocalRegs {
			for len(ap.LocalRegs[i].Ch) > 0 {
				<-ap.LocalRegs[i].Ch
			}
		}
	}

	for i := range ap.LocalRegs {
		select {
		case <-ap.LocalRegs[i].Ctx.Done():
		case ap.LocalRegs[i].Ch <- nil:
		}
	}
	if ap.ctr.bat != nil {
		ap.ctr.bat.Clean(proc.Mp())
		ap.ctr.bat = nil
	}
}
