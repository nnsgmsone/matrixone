package vector

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	FLAT     = iota // flat vector represent a uncompressed vector
	CONSTANT        // const vector
	DIST            // dictionary vector
)

// Vector represent a column
type Vector struct {
	class  int
	length int

	typ types.Type
	nsp *bitmap.Bitmap

	area []byte

	// data of fixed length element, in case of varlen, the Varlena
	col *reflect.SliceHeader
}

func (v *Vector) Reset() {
	v.length = 0
	v.area = v.area[:0]
}

func (v *Vector) Length() int {
	return v.length
}

func (v *Vector) SetLength(n int) {
	v.length = n
}

func (v *Vector) Size() int {
	return v.length*v.typ.TypeSize() + len(v.area)
}

func (v *Vector) GetType() types.Type {
	return v.typ
}

func (v *Vector) GetArea() []byte {
	return v.area
}

func (v *Vector) IsScalar() bool {
	return v.class == CONSTANT
}

func (v *Vector) IsScalarNull() bool {
	return v.IsScalar() && v.IsNull(0)
}

// HasNull used to detect the presence of null values
func (v *Vector) HasNull() bool {
	return v.nsp != nil && !v.nsp.IsEmpty()
}

// IsNull used to check if a line is null
func (v *Vector) IsNull(row uint64) bool {
	return v.nsp != nil && v.nsp.Contains(row)
}

func (v *Vector) AddNull(row uint64) {
	if v.nsp == nil {
		v.nsp = bitmap.New(int(row) + 1)
	}
	v.nsp.Add(row)
}

func (v *Vector) RemoveNull(row uint64) {
	if v.nsp != nil {
		v.nsp.Remove(row)
	}
}

func (v *Vector) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(uint8(v.class))
	{ // write length
		length := uint64(v.length)
		buf.Write(types.EncodeUint64(&length))
	}
	{ // write type
		data := types.EncodeType(&v.typ)
		buf.Write(data)
	}
	{
		if v.nsp != nil {
			data, err := v.nsp.Marshal()
			if err != nil {
				return nil, err
			}
			length := uint64(len(data))
			buf.Write(types.EncodeUint64(&length))
			if len(data) > 0 {
				buf.Write(data)
			}
		} else {
			length := uint64(0)
			buf.Write(types.EncodeUint64(&length))
		}
	}
	{ // write colLen, col
		data := (*(*[]byte)(unsafe.Pointer(v.col)))
		length := uint32(v.length * v.typ.TypeSize())
		if len(data[:length]) > 0 {
			buf.Write(data[:length])
		}
	}
	{ // write areaLen, area
		length := uint64(len(v.area))
		buf.Write(types.EncodeUint64(&length))
		if len(v.area) > 0 {
			buf.Write(v.area)
		}
	}
	return buf.Bytes(), nil
}

func (v *Vector) UnmarshalBinary(data []byte, mp *mpool.MPool) error {
	if v.col == nil {
		v.col = new(reflect.SliceHeader)
	}
	{ // read class
		v.class = int(data[0])
		data = data[1:]
	}
	{ // read length
		v.length = int(types.DecodeUint64(data[:8]))
		data = data[8:]
	}
	{ // read typ
		v.typ = types.DecodeType(data[:types.TSize])
		data = data[types.TSize:]
	}
	{ // read nsp
		size := types.DecodeUint64(data)
		data = data[8:]
		if size > 0 {
			v.nsp = new(bitmap.Bitmap)
			ndata := make([]byte, len(data))
			copy(ndata, data[:size])
			if err := v.nsp.Unmarshal(ndata); err != nil {
				return err
			}
			data = data[size:]
		}
	}
	{ // read col
		length := v.length * v.typ.TypeSize()
		if length > 0 {
			ndata, err := mp.Alloc(length)
			if err != nil {
				return err
			}
			copy(ndata, data[:length])
			*(*[]byte)(unsafe.Pointer(v.col)) = ndata
			data = data[length:]
		}
	}
	{ // read area
		length := types.DecodeUint64(data)
		data = data[8:]
		if length > 0 {
			ndata, err := mp.Alloc(int(length))
			if err != nil {
				return err
			}
			copy(ndata, data[:length])
			v.area = ndata
			data = data[:length]
		}
	}
	return nil
}

func (v *Vector) String() string {
	switch v.typ.Oid {
	case types.T_bool:
		return vecToString[bool](v)
	case types.T_int8:
		return vecToString[int8](v)
	case types.T_int16:
		return vecToString[int16](v)
	case types.T_int32:
		return vecToString[int32](v)
	case types.T_int64:
		return vecToString[int64](v)
	case types.T_float32:
		return vecToString[float32](v)
	case types.T_float64:
		return vecToString[float64](v)
	case types.T_char, types.T_varchar:
		col := getColumnValue[types.String](v)
		vs := make([]string, len(col))
		for i := range col {
			vs[i] = string((&col[i]).GetString(v.area))
		}
		if len(vs) == 1 {
			if v.IsNull(0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", vs[0])
			}
		}
		return fmt.Sprintf("%v-%s", vs, v.nsp)
	default:
		panic("vec to string unknown types.")
	}
}
