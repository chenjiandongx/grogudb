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

package uint64set

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloomFilterFromSets(t *testing.T) {
	const N = 10000

	sets := NewSets()
	set := sets.GetOrCreate("set1")
	for i := 0; i < N; i++ {
		set.Insert(uint64(i))
	}

	bf := NewBloomFilterFromSets(sets)
	var hint, miss int
	for i := 0; i < N*2; i++ {
		if bf.Test(uint64(i)) {
			hint++
		} else {
			miss++
		}
	}

	require.Equal(t, 10007, hint)
	require.Equal(t, 9993, miss)
	require.Equal(t, N, bf.Count())
}

func TestLoadBloomFilter(t *testing.T) {
	const N = 10000

	sets := NewSets()
	set := sets.GetOrCreate("set1")
	for i := 0; i < N; i++ {
		set.Insert(uint64(i))
	}

	bf := NewBloomFilterFromSets(sets)
	b := bf.Bytes()
	require.Equal(t, 18000, len(b))

	loaded, err := LoadBloomFilter(N, b)
	require.NoError(t, err)

	var hint, miss int
	for i := 0; i < N*2; i++ {
		if loaded.Test(uint64(i)) {
			hint++
		} else {
			miss++
		}
	}

	require.Equal(t, 10007, hint)
	require.Equal(t, 9993, miss)
	require.Equal(t, N, bf.Count())
}
