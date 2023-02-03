package batch

import (
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	consumers = 10
)

func TestBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := New(1)
	{
		vec := vector.New(vector.FLAT, types.New(types.T_int8))
		err := vector.Append(vec, int8(0), false, mp)
		require.NoError(t, err)
		bat.SetVector(0, vec)
	}
	cnt := int64(0)
	bat.DeliveredToConsumers(consumers)
	for i := 0; i < consumers; i++ {
		go func(b *Batch) {
			defer b.Relinquish()
			atomic.AddInt64(&cnt, int64(b.Length()))
		}(bat)
	}
	bat.WaitForAllAbstain()
	bat.Free(mp)
	require.Equal(t, int64(consumers), cnt)
	require.Equal(t, int64(0), mp.CurrNB())
}
