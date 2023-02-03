package types

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

func (str *String) GetString(area []byte) []byte {
	return area[str[0] : str[0]+str[1]]
}

func (str *String) SetString(data []byte, area []byte, mp *mpool.MPool) ([]byte, error) {
	vlen := len(data)
	voff := len(area)
	if voff+vlen >= cap(area) {
		var err error
		if area, err = mp.Grow(area, voff+vlen); err != nil {
			return nil, err
		}
	}
	area = append(area[:voff], data...)
	str[0], str[1] = uint32(voff), uint32(vlen)
	return area, nil
}
