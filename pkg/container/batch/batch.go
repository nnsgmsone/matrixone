package batch

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func New(n int) *Batch {
	return &Batch{
		vecs: make([]*vector.Vector, n),
	}
}

func (bat *Batch) Free(mp *mpool.MPool) {
	for _, vec := range bat.vecs {
		vec.Free(mp)
	}
	bat.vecs = nil
}

// DeliveredToConsumers used to send batch to multiple consumers temporarily
// and lose access to it briefly
func (bat *Batch) DeliveredToConsumers(n int) {
	bat.wg.Add(n)
}

// GiveUp used to allow a consumer to relinquish the use of the batch
func (bat *Batch) Relinquish() {
	bat.wg.Done()
}

// WaitForAllAbstain used to wait to regain access of batch
func (bat *Batch) WaitForAllAbstain() {
	bat.wg.Wait()
}
