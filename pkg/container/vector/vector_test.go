package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

const (
	benchRows = 1024
)

func TestLength(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8))
	err := AppendList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	require.Equal(t, 3, vec.Length())
	vec.SetLength(2)
	require.Equal(t, 2, vec.Length())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSize(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8))
	require.Equal(t, 0, vec.Size())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestConst(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(CONSTANT, types.New(types.T_int8))
	require.Equal(t, true, vec.IsScalar())
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestAppend(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_int8))
	err := Append(vec, int8(0), false, mp)
	require.NoError(t, err)
	err = Append(vec, int8(0), true, mp)
	require.NoError(t, err)
	err = AppendList(vec, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestAppendBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := New(FLAT, types.New(types.T_varchar))
	err := AppendBytes(vec, []byte("x"), false, mp)
	require.NoError(t, err)
	err = AppendBytes(vec, nil, true, mp)
	require.NoError(t, err)
	err = AppendBytesList(vec, [][]byte{[]byte("x"), []byte("y")}, nil, mp)
	require.NoError(t, err)
	vs := GetColumnValue[types.String](vec)
	area := vec.GetArea()
	for _, v := range vs {
		v.GetString(area)
	}
	vec.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestShrink(t *testing.T) {
	{
		mp := mpool.MustNewZero()
		v := New(FLAT, types.New(types.T_int8))
		err := AppendList(v, []int8{0, 1, 2}, nil, mp)
		require.NoError(t, err)
		sf := v.GetShrinkFunction()
		sf([]int64{1})
		require.Equal(t, []int8{1}, GetColumnValue[int8](v))
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}

}

func TestMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := New(FLAT, types.New(types.T_int8))
	err := AppendList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	v.AddNull(1)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data, mp)
	require.Equal(t, GetColumnValue[int8](v), GetColumnValue[int8](w))
	require.NoError(t, err)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestStrMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := New(FLAT, types.New(types.T_char))
	err := AppendBytesList(v, [][]byte{[]byte("x"), []byte("y")}, nil, mp)
	require.NoError(t, err)
	v.AddNull(1)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w := new(Vector)
	err = w.UnmarshalBinary(data, mp)
	require.NoError(t, err)
	{ // check data
		vs := GetColumnValue[types.String](v)
		ws := GetColumnValue[types.String](w)
		for i := range vs {
			require.Equal(t, vs[i].GetString(v.GetArea()), ws[i].GetString(w.GetArea()))
		}
	}
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}
