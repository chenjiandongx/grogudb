// Copyright 2023 The grogudb Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package slice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	t.Run("Append unique", func(t *testing.T) {
		slice := New()
		slice.Append(ID{0, 1}, []byte("foo"))
		slice.Append(ID{0, 2}, []byte("baz"))
		require.Equal(t, 2, slice.Count())
		require.Equal(t, 6, slice.Len())

		idx := 0
		slice.ForEach(func(b []byte) bool {
			switch idx {
			case 0:
				require.Equal(t, []byte("baz"), b)
			case 1:
				require.Equal(t, []byte("foo"), b)
			}
			idx++
			return false
		})
	})

	t.Run("Append duplicated", func(t *testing.T) {
		slice := New()
		slice.Append(ID{0, 1}, []byte("foo"))
		slice.Append(ID{0, 1}, []byte("baz"))
		require.Equal(t, 1, slice.Count())
		require.Equal(t, 3, slice.Len())

		slice.ForEach(func(b []byte) bool {
			require.Equal(t, []byte("baz"), b)
			return false
		})
	})

	t.Run("Frozen", func(t *testing.T) {
		slice := New()
		slice.Append(ID{0, 1}, []byte("foo"))
		slice.Append(ID{0, 2}, []byte("baz"))

		b := slice.Frozen()
		require.Equal(t, []byte("foobaz"), b[:len(b)-4])
	})

	t.Run("FrozenReverse", func(t *testing.T) {
		slice := New()
		slice.Append(ID{0, 1}, []byte("foo"))
		slice.Append(ID{0, 2}, []byte("baz"))

		b := slice.FrozenReverse()
		require.Equal(t, []byte("bazfoo"), b[:len(b)-4])
	})
}
