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
	"bytes"

	"github.com/bits-and-blooms/bloom/v3"

	"github.com/chenjiandongx/grogudb/pkg/binaryx"
)

const (
	defaultFalsePositiveRate = 0.001
)

// BloomFilter BloomFilter 定义
// 使用尽量少的内存快速判断 key 是否在 disk segment 中
type BloomFilter interface {
	Test(k uint64) bool
	Bytes() []byte
	Count() int
}

type bloomFilter struct {
	n      int
	filter *bloom.BloomFilter
}

// Test 测试 k 是否存在
func (b *bloomFilter) Test(k uint64) bool {
	return b.filter.Test(binaryx.PutUint64(k))
}

// Count 返回元素个数
func (b *bloomFilter) Count() int {
	return b.n
}

// Bytes 返回字节数组
func (b *bloomFilter) Bytes() []byte {
	buf := &bytes.Buffer{}
	_, _ = b.filter.WriteTo(buf)
	return buf.Bytes()
}

// NewBloomFilterFromSets 将 *Sets 转换为 BloomFilter
func NewBloomFilterFromSets(sets *Sets) BloomFilter {
	n := sets.CountAll()
	filter := bloom.NewWithEstimates(uint(n), defaultFalsePositiveRate)

	sets.IterAllKeys(func(k uint64) {
		filter.Add(binaryx.PutUint64(k))
	})

	return &bloomFilter{
		n:      n,
		filter: filter,
	}
}

// LoadBloomFilter 读取字节数组并转换为 BloomFilter
func LoadBloomFilter(n int, b []byte) (BloomFilter, error) {
	buf := bytes.NewBuffer(b)

	filter := bloom.NewWithEstimates(uint(n), defaultFalsePositiveRate)
	_, err := filter.ReadFrom(buf)
	if err != nil {
		return nil, err
	}
	return &bloomFilter{
		n:      n,
		filter: filter,
	}, nil
}
