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
	"sync"

	dll "github.com/emirpasic/gods/lists/doublylinkedlist"

	"github.com/chenjiandongx/grogudb/pkg/codec"
)

type ID struct {
	Flag uint8
	Hash uint64
}

// Slice 内部使用的是双向链表的数据结构（尽量减少额外内存分配）
// 对外表现是 Slice 的行为
type Slice struct {
	mut    sync.RWMutex
	length int
	list   *dll.List
	pos    map[ID]uint32 // 省吃俭用 4 个字节
}

// New 创建一个新的 Slice 实例
func New() *Slice {
	return &Slice{
		list: dll.New(),
		pos:  make(map[ID]uint32),
	}
}

// Append 追加 b 至 Slice
func (s *Slice) Append(id ID, b []byte) int {
	s.mut.Lock()
	defer s.mut.Unlock()

	var l int

	cur, ok := s.pos[id]
	if ok {
		obj, _ := s.list.Get(int(cur))
		val := obj.([]byte)
		l -= len(val)
		s.list.Remove(int(cur))
		s.list.Append(b)
		s.pos[id] = uint32(s.list.Size() - 1)
	} else {
		s.pos[id] = uint32(s.list.Size())
		s.list.Append(b)
	}

	l += len(b)
	s.length += l

	return l
}

// Len 返回 Slice Bytes 总长度
func (s *Slice) Len() int {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.length
}

// Count 返回 Slice 元素个数
func (s *Slice) Count() int {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.list.Size()
}

// FrozenReverse 重置 Slice 返回倒序的 []byte 同时会补充 crc32 checksum
func (s *Slice) FrozenReverse() []byte {
	s.mut.Lock()
	defer s.mut.Unlock()

	dst := make([]byte, 0, s.length+codec.SizeChecksum)
	it := s.list.Iterator()
	for it.End(); it.Prev(); {
		obj := it.Value()
		val := obj.([]byte)
		dst = append(dst, val...)
	}

	checksum := codec.CRC32(dst[:])
	dst = append(dst, checksum...)

	s.reset()
	return dst
}

// Frozen 重置 Slice 返回正序的 []byte 同时会补充 crc32 checksum
func (s *Slice) Frozen() []byte {
	s.mut.Lock()
	defer s.mut.Unlock()

	dst := make([]byte, 0, s.length+codec.SizeChecksum)
	it := s.list.Iterator()
	for it.Begin(); it.Next(); {
		obj := it.Value()
		val := obj.([]byte)
		dst = append(dst, val...)
	}

	checksum := codec.CRC32(dst[:])
	dst = append(dst, checksum...)

	s.reset()
	return dst
}

func (s *Slice) reset() {
	s.list.Clear()
	s.length = 0
	s.pos = make(map[ID]uint32)
}

// ForEach 遍历每一个 Item 并执行 visitFn 函数
func (s *Slice) ForEach(visitFn func(b []byte) bool) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	it := s.list.Iterator()
	for it.End(); it.Prev(); {
		obj := it.Value()
		val := obj.([]byte)
		if visitFn(val) {
			break
		}
	}
}

// CopyForEach 复制遍历每一个 Item 并执行 visitFn 函数
// 避免锁占用太长时间 影响写入
func (s *Slice) CopyForEach(visitFn func(b []byte) bool) {
	s.mut.RLock()
	bs := make([][]byte, 0, s.Count())
	it := s.list.Iterator()
	for it.End(); it.Prev(); {
		obj := it.Value()
		val := obj.([]byte)
		bs = append(bs, val)
	}
	s.mut.RUnlock()

	for _, item := range bs {
		if visitFn(item) {
			break
		}
	}
}
