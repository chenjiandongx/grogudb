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

package binaryx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinary(t *testing.T) {
	n := 0x0A

	t.Run("Uint32", func(t *testing.T) {
		b := PutUint32(uint32(n))
		require.Equal(t, uint32(n), Uint32(b))
	})

	t.Run("Uint64", func(t *testing.T) {
		b := PutUint64(uint64(n))
		require.Equal(t, uint64(n), Uint64(b))
	})
}
