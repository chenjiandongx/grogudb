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

import "encoding/binary"

// PutUint32 编码 uint32
func PutUint32(n uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	return b
}

// Uint32 解码 uint32
func Uint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

// PutUint64 编码 uint64
func PutUint64(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}

// Uint64 解码 uint64
func Uint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
