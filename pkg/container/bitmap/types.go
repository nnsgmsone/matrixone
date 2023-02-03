package bitmap

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Nulls represent line numbers of tuple's is null
type Bitmap struct {
	isEmpty bool
	// len represent the size of bitmap
	len  int
	data []uint64
}

func (n *Bitmap) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	length := uint64(n.len)
	buf.Write(types.EncodeUint64(&length))
	length = uint64(len(n.data) * 8)
	buf.Write(types.EncodeUint64(&length))
	buf.Write(types.EncodeSlice(n.data, 8))
	return buf.Bytes(), nil
}

func (n *Bitmap) Unmarshal(data []byte) error {
	n.isEmpty = false
	n.IsEmpty()
	n.len = int(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	n.data = types.DecodeSlice[uint64](data[:size], 8)
	return nil
}

func (n *Bitmap) String() string {
	return fmt.Sprintf("%v", n.ToArray())
}
