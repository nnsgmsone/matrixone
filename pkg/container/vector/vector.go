package vector

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func New(class int, typ types.Type) *Vector {
	return &Vector{
		typ:   typ,
		class: class,
		col:   new(reflect.SliceHeader),
	}
}

func (v *Vector) Free(mp *mpool.MPool) {
	if v.col != nil {
		mp.Free((*(*[]byte)(unsafe.Pointer(v.col))))
	}
	if v.area != nil {
		mp.Free(v.area)
	}
	v.col = nil
	v.area = nil
}

// PreExtend use to expand the capacity of the vector
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}
	switch v.typ.Oid {
	case types.T_bool:
		return extend[bool](v, rows, mp)
	case types.T_int8:
		return extend[int8](v, rows, mp)
	case types.T_int16:
		return extend[int16](v, rows, mp)
	case types.T_int32:
		return extend[int32](v, rows, mp)
	case types.T_int64:
		return extend[int64](v, rows, mp)
	case types.T_float32:
		return extend[float32](v, rows, mp)
	case types.T_float64:
		return extend[float64](v, rows, mp)
	case types.T_char, types.T_varchar:
		return extend[types.String](v, rows, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.PreExtend", v.typ))
	}
}

func (v *Vector) GetShrinkFunction() func([]int64) {
	if v.class != FLAT {
		return func(sels []int64) {
			v.length = len(sels)
		}
	}
	switch v.GetType().Oid {
	case types.T_bool:
		return func(sels []int64) {
			shrinkFixed[bool](v, sels)
		}
	case types.T_int8:
		return func(sels []int64) {
			shrinkFixed[int8](v, sels)
		}
	case types.T_int16:
		return func(sels []int64) {
			shrinkFixed[int16](v, sels)
		}
	case types.T_int32:
		return func(sels []int64) {
			shrinkFixed[int32](v, sels)
		}
	case types.T_int64:
		return func(sels []int64) {
			shrinkFixed[int64](v, sels)
		}
	case types.T_float32:
		return func(sels []int64) {
			shrinkFixed[float32](v, sels)
		}
	case types.T_float64:
		return func(sels []int64) {
			shrinkFixed[float64](v, sels)
		}
	case types.T_char, types.T_varchar:
		return func(sels []int64) {
			shrinkFixed[types.String](v, sels)
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetUnionFunction", v.typ))
	}
}

func GetColumnValue[T any](v *Vector) []T {
	return getColumnValue[T](v)
}

func Append[T any](v *Vector, w T, isNull bool, mp *mpool.MPool) error {
	return appendOne(v, w, isNull, mp)
}

func AppendBytes(v *Vector, w []byte, isNull bool, mp *mpool.MPool) error {
	if isNull {
		return appendOneBytes(v, nil, true, mp)
	}
	return appendOneBytes(v, w, false, mp)
}

func AppendList[T any](v *Vector, ws []T, nsp *bitmap.Bitmap, mp *mpool.MPool) error {
	return appendList(v, ws, nsp, mp)
}

func AppendBytesList(v *Vector, ws [][]byte, nsp *bitmap.Bitmap, mp *mpool.MPool) error {
	return appendBytesList(v, ws, nsp, mp)
}

func appendOne[T any](v *Vector, w T, isNull bool, mp *mpool.MPool) error {
	if err := extend[T](v, 1, mp); err != nil {
		return err
	}
	length := v.length
	v.length++
	col := getColumnValue[T](v)
	if isNull {
		if v.nsp == nil {
			v.nsp = bitmap.New(v.length)
		}
		v.nsp.Add(uint64(v.length))
	} else {
		col[length] = w
	}
	return nil
}

func appendOneBytes(v *Vector, bs []byte, isNull bool, mp *mpool.MPool) error {
	var err error
	var area []byte
	var str types.String

	if isNull {
		return appendOne(v, str, true, mp)
	} else {
		if area, err = (&str).SetString(bs, v.area, mp); err != nil {
			return err
		}
		v.area = area
		return appendOne(v, str, false, mp)
	}
}

func appendList[T any](v *Vector, ws []T, nsp *bitmap.Bitmap, mp *mpool.MPool) error {
	if err := extend[T](v, len(ws), mp); err != nil {
		return err
	}
	length := v.length
	v.length += len(ws)
	col := getColumnValue[T](v)
	for i, w := range ws {
		if nsp != nil && nsp.Contains(uint64(i)) {
			if v.nsp == nil {
				v.nsp = bitmap.New(i + length)
			}
			v.nsp.Add(uint64(i + length))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(v *Vector, ws [][]byte, nsp *bitmap.Bitmap, mp *mpool.MPool) error {
	var err error
	var area []byte
	var str types.String

	if err = extend[types.String](v, len(ws), mp); err != nil {
		return err
	}
	length := v.length
	v.length += len(ws)
	col := getColumnValue[types.String](v)
	for i, w := range ws {
		if nsp != nil && nsp.Contains(uint64(i)) {
			if v.nsp == nil {
				v.nsp = bitmap.New(i + length)
			}
			v.nsp.Add(uint64(i + length))
		} else {
			if area, err = (&str).SetString(w, v.area, mp); err != nil {
				return err
			}
			v.area = area
			col[length+i] = str
		}
	}
	return nil
}

func extend[T any](v *Vector, rows int, mp *mpool.MPool) error {
	sz := v.typ.TypeSize()
	if length := v.length + rows; length >= v.col.Cap/sz {
		data := (*(*[]byte)(unsafe.Pointer(v.col)))[:v.length*sz]
		ndata, err := mp.Grow(data, length*sz)
		if err != nil {
			return err
		}
		*(*[]byte)(unsafe.Pointer(v.col)) = ndata
	}
	return nil
}

func shrinkFixed[T any](v *Vector, sels []int64) {
	vs := getColumnValue[T](v)
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	if v.nsp != nil {
		v.nsp = v.nsp.Filter(sels)
	}
	v.length = len(sels)
}

func getColumnValue[T any](v *Vector) []T {
	if v.class == CONSTANT {
		vs := (*(*[]T)(unsafe.Pointer(v.col)))[:1]
		rs := make([]T, v.length)
		for i := range rs {
			rs[i] = vs[0]
		}
		return rs
	}
	return (*(*[]T)(unsafe.Pointer(v.col)))[:v.length]
}

func vecToString[T any](v *Vector) string {
	col := getColumnValue[T](v)
	if len(col) == 1 {
		if v.IsNull(0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	return fmt.Sprintf("%v-%s", col, v.nsp)
}
