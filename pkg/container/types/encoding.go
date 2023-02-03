package types

import (
	"bytes"
	"encoding/gob"
	"unsafe"
)

const (
	TSize int = int(unsafe.Sizeof(Type{}))
)

func EncodeSlice[T any](v []T, sz int) (ret []byte) {
	if len(v) > 0 {
		ret = unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), cap(v)*sz)[:len(v)*sz]
	}
	return
}

func DecodeSlice[T any](v []byte, sz int) (ret []T) {
	if len(v) > 0 {
		ret = unsafe.Slice((*T)(unsafe.Pointer(&v[0])), cap(v)/sz)[:len(v)/sz]
	}
	return
}

func Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func EncodeType(v *Type) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), TSize)
}

func DecodeType(v []byte) Type {
	return *(*Type)(unsafe.Pointer(&v[0]))
}

func EncodeUint64(v *uint64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

func DecodeUint64(v []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&v[0]))
}
