package bitmap

import (
	"math/bits"
)

func New(n int) *Bitmap {
	return &Bitmap{
		len:  n,
		data: make([]uint64, (n-1)/64+1),
	}
}

func (n *Bitmap) Clear() {
	for i := range n.data {
		n.data[i] = 0
	}
}

func (n *Bitmap) Len() int {
	return n.len
}

func (n *Bitmap) Size() int {
	return len(n.data) * 8
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (n *Bitmap) IsEmpty() bool {
	if n.isEmpty {
		return true
	}
	for i := 0; i < len(n.data); i++ {
		if n.data[i] != 0 {
			return false
		}
	}
	n.isEmpty = true
	return true
}

func (n *Bitmap) Add(row uint64) {
	n.isEmpty = true
	n.TryExpandWithSize(int(row) + 1)
	n.data[row>>6] |= 1 << (row & 0x3F)
}

func (n *Bitmap) Remove(row uint64) {
	n.data[row>>6] &^= (uint64(1) << (row & 0x3F))
}

// Contains returns true if the row is contained in the Bitmap
func (n *Bitmap) Contains(row uint64) bool {
	if row >= uint64(n.len) {
		return false
	}
	return (n.data[row>>6] & (1 << (row & 0x3F))) != 0
}

func (n *Bitmap) Or(m *Bitmap) {
	n.TryExpand(m)
	for i := 0; i < len(n.data); i++ {
		n.data[i] |= m.data[i]
	}
}

func (n *Bitmap) TryExpand(m *Bitmap) {
	if n.len < m.len {
		n.Expand(m.len)
	}
}

func (n *Bitmap) TryExpandWithSize(size int) {
	if n.len <= size {
		n.Expand(size)
	}
}

func (n *Bitmap) Expand(size int) {
	data := make([]uint64, (size-1)/64+1)
	copy(data, n.data)
	n.len = size
	n.data = data
}

func (n *Bitmap) Filter(sels []int64) *Bitmap {
	m := New(n.len)
	for i, sel := range sels {
		if n.Contains(uint64(sel)) {
			m.Add(uint64(i))
		}
	}
	return m
}

func (n *Bitmap) Count() int {
	var cnt int

	for i := 0; i < len(n.data); i++ {
		cnt += bits.OnesCount64(n.data[i])
	}
	return cnt
}

func (n *Bitmap) ToArray() []uint64 {
	var rows []uint64

	start := uint64(0)
	for i := 0; i < len(n.data); i++ {
		bit := n.data[i]
		for bit != 0 {
			t := bit & -bit
			rows = append(rows, start+uint64(bits.OnesCount64(t-1)))
			bit ^= t
		}
		start += 64
	}
	return rows
}
