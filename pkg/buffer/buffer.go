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

package buffer

import (
	"bytes"
	"hash/crc32"
	"sync"

	"github.com/chenjiandongx/grogudb/pkg/binaryx"
)

var bufPool = sync.Pool{New: func() interface{} {
	return &Buffer{
		buf: &bytes.Buffer{},
	}
}}

// Get 从 Pool 中取 *Buffer
func Get() *Buffer {
	return bufPool.Get().(*Buffer)
}

// Put 将 *Buffer 放置到 Pool
func Put(buf *Buffer) {
	buf.Reset()
	bufPool.Put(buf)
}

// Buffer 可复用的 *Buffer
type Buffer struct {
	buf *bytes.Buffer
}

// Len 返回 buffer 长度
func (b *Buffer) Len() int {
	return b.buf.Len()
}

// Writer 将 bs 写入到 buffer 中
func (b *Buffer) Write(bs []byte) (int, error) {
	return b.buf.Write(bs)
}

// Reset 重置 buffer
func (b *Buffer) Reset() {
	b.buf.Reset()
}

// Bytes 返回字节数据
func (b *Buffer) Bytes() []byte {
	return b.buf.Bytes()
}

// Frozen 冻结 buffer 并补充 crc32 checksum
func (b *Buffer) Frozen() []byte {
	checksum := crc32.ChecksumIEEE(b.Bytes())
	b.buf.Write(binaryx.PutUint32(checksum))
	return b.buf.Bytes()
}
