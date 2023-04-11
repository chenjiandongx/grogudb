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

package grogudb

import (
	"errors"

	"github.com/chenjiandongx/grogudb/pkg/codec"
	"github.com/chenjiandongx/grogudb/pkg/uint64set"
)

var (
	ErrEmtpyRecordKey   = errors.New("grogudb/bucket: empty record key")
	ErrEmtpyRecordValue = errors.New("grogudb/bucket: empty record value")
)

func validateRecordKV(k, v []byte) error {
	if len(k) == 0 {
		return ErrEmtpyRecordKey
	}
	if len(v) == 0 {
		return ErrEmtpyRecordValue
	}
	return nil
}

func validateRecordKey(k []byte) error {
	if len(k) == 0 {
		return ErrEmtpyRecordKey
	}
	return nil
}

// Bucket 是为了对存储 Key/Value 做 shard 相当于数据库中的 Table
// Bucket 中包含了 memorySegment 处理实时写入 同时反向持有 *DB 实例的 diskSegment 列表
type Bucket struct {
	name            string
	keys            *uint64set.Set
	head            *memorySegment
	statistics      *statistics
	getIterReleaser func() *iterReleaser
}

// Count 返回 Bucket Keys 数量
func (b *Bucket) Count() int {
	return b.keys.Count()
}

// Clear 清空 Bucket 所有 key
func (b *Bucket) Clear() {
	b.statistics.clear.Add(1)
	b.keys.Clear()
	b.head.Clear()
}

// Has 判断 Key 是否存在
func (b *Bucket) Has(key []byte) bool {
	if len(key) == 0 {
		return false
	}

	return b.keys.Has(codec.HashKey(key))
}

// PutIf 当 Key 不存在的时候设置 Key/Value Key 存在时不做操作
func (b *Bucket) PutIf(key, val []byte) error {
	if err := validateRecordKV(key, val); err != nil {
		return err
	}
	b.head.PutIf(key, val)
	return nil
}

// Put 新增 Key/Value 记录
func (b *Bucket) Put(key, val []byte) error {
	if err := validateRecordKV(key, val); err != nil {
		return err
	}

	b.statistics.put.Add(1)
	b.head.Put(key, val)
	return nil
}

// Del 删除指定 Key
func (b *Bucket) Del(key []byte) error {
	if err := validateRecordKey(key); err != nil {
		return err
	}

	b.statistics.del.Add(1)
	b.head.Del(key)
	return nil
}

// Get 返回指定 Key 对应的 Value
//
// Get 返回的数据不允许直接修改，如果有修改需求 请使用 .Copy() 复制后的数据
// Get 是一个开销`相对高昂`的操作，查询 key 有 3 种情况
//  1. key 不存在，直接返回，无 IO
//  2. key 存在，在 memory segment 中检索，key 命中，无 IO
//  3. key 存在，在 memory segment 未命中，退避到 disk segment 检索
//     由于 key 是没有排序的，因此必须按序扫描所有的 block 直至找到，此时会有读放大的情况（比如为了查找 10B 数据而扫描了 2MB 的 datablock）
//     同时 disk segment 的搜索已经做了一些措施来尽量避免陷入 IO，如提前判断 key 是否存在，bloomfilter 加速过滤...
//
// Note:
//  1. 尝试过使用 LRU 来缓存热区的部分 datablock，但在随机查询的时候，这种方式频繁的换入换出反而带来额外的开销
//     因为几 MB 的 buffer 区几乎每次都 miss 了，难以满足 GB+ 的数据的查询需求
func (b *Bucket) Get(key []byte) (Bytes, error) {
	if err := validateRecordKey(key); err != nil {
		return nil, err
	}

	if !b.keys.Has(codec.HashKey(key)) {
		return nil, nil
	}

	var val []byte
	var err error

	ir := b.getIterReleaser() // 锁定 disk segments 快照
	defer ir.release()

	// 优先从 memory segment 检索
	val, ok := b.head.Get(key)
	if ok {
		return val, nil
	}

	ir.iter(func(seg *diskSegment) bool {
		val, err = seg.Get(b.name, key)
		if err != nil {
			return true
		}

		// val 不为空代表已经检索到 需要退出循环
		return val != nil
	})

	return val, err
}

// Range 遍历每个 Key 并执行 fn 方法
//
// Range 返回的数据不允许直接修改 如果有修改需求 请使用 .Copy() 复制后的数据
// 请勿在 Range 内调用 Bucket 其他 API 避免死锁
func (b *Bucket) Range(fn func(key, val Bytes)) error {
	return b.bucketRange(false, fn)
}

// FastRange 拷贝 memory segment 元素并遍历每个 Key 并执行 fn 方法
//
// 避免长期占用锁影响写入 但同时会带来一定的内存开销
// Range 返回的数据不允许直接修改 如果有修改需求 请使用 .Copy() 复制后的数据
// 请勿在 Range 内调用 Bucket 其他 API 避免死锁
//
// TODO(optimize): 考虑使用 tempfile 代替内存拷贝 牺牲 IO 来减少内存使用
func (b *Bucket) FastRange(fn func(key, val Bytes)) error {
	return b.bucketRange(true, fn)
}

func (b *Bucket) bucketRange(ifCopy bool, fn func(key, val Bytes)) error {
	visited := uint64set.NewSet()
	deleted := uint64set.NewSet()
	pass := func(flag codec.Flag, key uint64) bool {
		// 记录删除的 key
		if flag == codec.FlagDel && !deleted.Has(key) {
			deleted.Insert(key)
		}
		// 如果 key 被删除 无须再访问
		if deleted.Has(key) {
			return false
		}

		if visited.Has(key) {
			return false
		}
		visited.Insert(key)
		return true
	}

	// 优先遍历 memory segment 然后再是 disk segment
	// 遍历是取当刻快照
	var err error

	ir := b.getIterReleaser() // 锁定 disk segments 快照
	defer ir.release()

	b.head.Range(ifCopy, fn, pass)
	ir.iter(func(seg *diskSegment) bool {
		err = seg.Range(b.name, func(flag codec.Flag, key, val []byte, n int) bool {
			fn(key, val)
			return false
		}, pass)

		// 错误则退出后续 disk segment 迭代
		return err != nil
	})

	return err
}
