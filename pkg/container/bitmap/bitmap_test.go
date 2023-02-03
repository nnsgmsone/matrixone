// Copyright (C) 2021 nexus.
//
// This file is part of nexus
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package bitmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10
	BenchmarkRows = 8192
)

func TestNulls(t *testing.T) {
	np := New(Rows)
	require.Equal(t, 0, np.Count())
	np.Clear()

	ok := np.IsEmpty()
	require.Equal(t, true, ok)
	np.Add(0)
	ok = np.Contains(0)
	require.Equal(t, true, ok)
	require.Equal(t, 1, np.Count())

	np.Remove(0)
	ok = np.IsEmpty()
	require.Equal(t, true, ok)

	nq := New(Rows)
	data, err := np.Marshal()
	require.NoError(t, err)
	err = nq.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, np.ToArray(), nq.ToArray())

	np.Clear()
}

func BenchmarkAdd(b *testing.B) {
	np := New(BenchmarkRows)
	for i := 0; i < b.N; i++ {
		for j := 0; j < BenchmarkRows; j++ {
			np.Add(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Contains(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Remove(uint64(j))
		}
	}
}
