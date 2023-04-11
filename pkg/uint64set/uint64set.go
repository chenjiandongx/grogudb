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
	"sync"

	cmap "github.com/orcaman/concurrent-map/v2"
)

// Set 是线程安全的 uint64 集合
type Set struct {
	set cmap.ConcurrentMap[uint64, struct{}]
}

// NewSet 生成并返回 Set 实例
func NewSet() *Set {
	var shard uint64 = 32
	set := cmap.NewWithCustomShardingFunction[uint64, struct{}](func(key uint64) uint32 {
		return uint32(key % shard)
	})
	return &Set{set: set}
}

// Count 返回集合元素个数
func (s *Set) Count() int {
	return s.set.Count()
}

// Insert 新增元素
func (s *Set) Insert(n uint64) {
	s.set.Set(n, struct{}{})
}

// Remove 移除元素
func (s *Set) Remove(n uint64) {
	s.set.Remove(n)
}

// Clear 清除所有元素
func (s *Set) Clear() {
	s.set.Clear()
}

// Has 判断元素是否存在
func (s *Set) Has(n uint64) bool {
	return s.set.Has(n)
}

// Keys 返回元素列表
func (s *Set) Keys() []uint64 {
	return s.set.Keys()
}

// Merge 合并 *Set
func (s *Set) Merge(set *Set) {
	if set == nil {
		return
	}
	for _, key := range set.set.Keys() {
		s.Insert(key)
	}
}

// Sets 管理着多个 *Set 示例
type Sets struct {
	mut  sync.RWMutex
	sets map[string]*Set
}

// NewSets 生成并返回 *Sets 实例
func NewSets() *Sets {
	return &Sets{sets: make(map[string]*Set)}
}

// AsBloomFilter 将 Sets 转换为 BloomFilter
func (ss *Sets) AsBloomFilter() BloomFilter {
	return NewBloomFilterFromSets(ss)
}

// HasKey 判断 Sets[name] 中是否存在 key
func (ss *Sets) HasKey(name string, key uint64) bool {
	ss.mut.RLock()
	defer ss.mut.RUnlock()

	set, ok := ss.sets[name]
	if !ok {
		return false
	}
	return set.Has(key)
}

// Remove 删除指定 name 的 Set 实例
func (ss *Sets) Remove(name string) {
	ss.mut.Lock()
	defer ss.mut.Unlock()

	delete(ss.sets, name)
}

// Count 返回指定 name 的 Set 元素个数
func (ss *Sets) Count(name string) int {
	ss.mut.RLock()
	defer ss.mut.RUnlock()

	set, ok := ss.sets[name]
	if !ok {
		return 0
	}
	return set.Count()
}

// CountAll 返回 Sets 中所有元素个数
func (ss *Sets) CountAll() int {
	ss.mut.RLock()
	defer ss.mut.RUnlock()

	var total int
	for _, set := range ss.sets {
		total += set.Count()
	}
	return total
}

// IterAllKeys 遍历所有的 key 并执行 fn 方法
func (ss *Sets) IterAllKeys(fn func(k uint64)) {
	ss.mut.RLock()
	defer ss.mut.RUnlock()

	for _, set := range ss.sets {
		for _, k := range set.Keys() {
			fn(k)
		}
	}
}

// Update 更新 Set
func (ss *Sets) Update(name string, set *Set) {
	ss.mut.Lock()
	defer ss.mut.Unlock()

	ss.sets[name] = set
}

// Get 获取 Set
func (ss *Sets) Get(name string) *Set {
	ss.mut.RLock()
	defer ss.mut.RUnlock()

	return ss.sets[name]
}

// GetOrCreate 获取或创建 Set 实例
func (ss *Sets) GetOrCreate(name string) *Set {
	var set *Set
	ss.mut.RLock()
	set = ss.sets[name]
	ss.mut.RUnlock()

	if set != nil {
		return set
	}

	ss.mut.Lock()
	defer ss.mut.Unlock()

	if set = ss.sets[name]; set != nil {
		return set
	}

	set = NewSet()
	ss.sets[name] = set
	return set
}
