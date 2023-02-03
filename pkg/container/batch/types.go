package batch

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Batch struct {
	vecs []*vector.Vector
	// wg used to wait for reuse of the batch
	wg sync.WaitGroup
}

func (bat *Batch) Reset() {
	for _, vec := range bat.vecs {
		vec.Reset()
	}
}

func (bat *Batch) Length() int {
	if len(bat.vecs) == 0 {
		return 0
	}
	return bat.vecs[0].Length()
}

func (bat *Batch) SetLength(n int) {
	for _, vec := range bat.vecs {
		vec.SetLength(n)
	}
}

func (bat *Batch) VectorCount() int {
	return len(bat.vecs)
}

func (bat *Batch) GetVector(i int) *vector.Vector {
	return bat.vecs[i]
}

func (bat *Batch) SetVector(i int, vec *vector.Vector) {
	bat.vecs[i] = vec
}
