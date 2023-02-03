package types

import (
	"fmt"
)

type T uint8

const (
	// bool family
	T_bool = iota

	// integer family
	T_int8
	T_int16
	T_int32
	T_int64

	// float family
	T_float32
	T_float64

	// string family
	T_char
	T_varchar
)

type Type struct {
	Oid   T
	Seat0 uint8
	Seat1 uint8
	Seat2 uint8
	Size  int32 // e.g. int32.Size = 4, int64.Size = 8
}

type String [2]uint32

func New(oid T) Type {
	return Type{Oid: oid, Size: int32(typeSize(oid))}
}

func (t Type) TypeSize() int {
	return typeSize(t.Oid)
}

func (t Type) String() string {
	return t.Oid.String()
}

func (t T) String() string {
	switch t {
	case T_bool:
		return "BOOL"
	case T_int8:
		return "TINYINT"
	case T_int16:
		return "SMALLINT"
	case T_int32:
		return "INT"
	case T_int64:
		return "BIGINT"
	case T_float32:
		return "FLOAT"
	case T_float64:
		return "DOUBLE"
	case T_char:
		return "CHAR"
	case T_varchar:
		return "VARCHAR"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}

func typeSize(oid T) int {
	switch oid {
	case T_bool:
		return 1
	case T_int8:
		return 1
	case T_int16:
		return 2
	case T_int32, T_float32:
		return 4
	case T_int64, T_float64:
		return 8
	case T_char, T_varchar:
		return 8
	}
	return -1
}
