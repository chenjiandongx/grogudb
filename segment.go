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
	"bytes"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"

	"github.com/chenjiandongx/grogudb/pkg/codec"
	"github.com/chenjiandongx/grogudb/pkg/fsx"
	"github.com/chenjiandongx/grogudb/pkg/slice"
	"github.com/chenjiandongx/grogudb/pkg/uint64set"
)

// memSegCallback memory segment 回调函数列表
type memSegCallback struct {
	onRemove func(h uint64)
	onInsert func(h uint64)
	onExist  func(h uint64) bool
	onBytes  func(n int64)
}

// memorySegment 代表着内存 segment 即类似 LSM-Tree 中的 level0
//
// 所有数据优先写入到 memory segment 再定期 flush 到磁盘
type memorySegment struct {
	name      string
	mut       sync.RWMutex
	keySet    *uint64set.Set
	cb        memSegCallback
	clearAt   int64
	dataBytes *slice.Slice
	keysBytes *slice.Slice
}

func newMemorySegment(name string, cb memSegCallback) *memorySegment {
	return &memorySegment{
		name:      name,
		dataBytes: slice.New(),
		keysBytes: slice.New(),
		keySet:    uint64set.NewSet(),
		cb:        cb,
	}
}

// Len 返回 dataBytes 长度
func (ms *memorySegment) Len() int {
	return ms.dataBytes.Len()
}

// Flush 冻结 ByteSlice 同时将 keys 置空
func (ms *memorySegment) Flush() ([]byte, []byte, *uint64set.Set) {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	keySet := ms.keySet
	ms.keySet = uint64set.NewSet()

	return ms.dataBytes.FrozenReverse(), ms.keysBytes.Frozen(), keySet
}

// Clear 清空 keys
func (ms *memorySegment) Clear() {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	sid := slice.ID{Flag: uint8(codec.FlagTombstone)}
	encodedKey := codec.EncodeKeyEntity(codec.FlagTombstone, 0, 0)
	ms.keysBytes.Append(sid, encodedKey)

	encodedRecord := codec.EncodeRecord(codec.FlagTombstone, nil, nil)
	ms.dataBytes.Append(sid, encodedRecord)
	ms.keySet.Clear()

	nano := time.Now().UnixNano()
	ms.clearAt = nano
}

func (ms *memorySegment) append(op operation, key, val []byte) {
	h := codec.HashKey(key)
	exist := ms.cb.onExist(h)
	if (op == opPutIf && exist) || (op == opDel && !exist) {
		return
	}

	flag := codec.FlagPut
	if op == opDel {
		flag = codec.FlagDel
	}

	encodedRecord := codec.EncodeRecord(flag, key, val)
	l := int64(len(encodedRecord))

	encodedKey := codec.EncodeKeyEntity(flag, h, uint32(l))

	sid := slice.ID{Flag: uint8(flag), Hash: h}
	var delta int

	ms.mut.Lock()
	switch op {
	case opPut:
		ms.keySet.Insert(h)
		ms.cb.onInsert(h)
		delta = ms.dataBytes.Append(sid, encodedRecord)
		ms.keysBytes.Append(sid, encodedKey)

	case opPutIf:
		if !ms.cb.onExist(h) {
			ms.keySet.Insert(h)
			ms.cb.onInsert(h)
			delta = ms.dataBytes.Append(sid, encodedRecord)
			ms.keysBytes.Append(sid, encodedKey)
		}

	case opDel:
		if ms.cb.onExist(h) {
			ms.keySet.Remove(h)
			ms.cb.onRemove(h)
			delta = ms.dataBytes.Append(sid, encodedRecord)
			ms.keysBytes.Append(sid, encodedKey)
		}
	}
	ms.mut.Unlock()
	ms.cb.onBytes(int64(delta))
}

// PutIf 当 Key 不存在的时候设置 Key/Value Key 存在时不做操作
func (ms *memorySegment) PutIf(key, val []byte) {
	ms.append(opPutIf, key, val)
}

// Put 新增 Key/Value 记录
func (ms *memorySegment) Put(key, val []byte) {
	ms.append(opPut, key, val)
}

// Del 删除指定 Key
func (ms *memorySegment) Del(key []byte) {
	ms.append(opDel, key, nil)
}

// Range 遍历每个 Key 并执行 fn 方法
// pass 用于判断是否跳出 range 循环
func (ms *memorySegment) Range(ifCopy bool, fn func(key, val Bytes), pass codec.PassFunc) {
	ms.mut.RLock()
	defer ms.mut.RUnlock()

	rangeFn := ms.dataBytes.ForEach
	if ifCopy {
		rangeFn = ms.dataBytes.CopyForEach
	}

	rangeFn(func(b []byte) bool {
		flag, key, val, err := codec.DecodeRecord(b)
		if err != nil {
			return true
		}

		if flag == codec.FlagTombstone {
			return true
		}
		if pass(flag, codec.HashKey(key)) {
			fn(key, val)
		}
		return false
	})
}

// Get 返回指定 Key 对应的 Value
func (ms *memorySegment) Get(key []byte) ([]byte, bool) {
	ms.mut.RLock()
	defer ms.mut.RUnlock()

	h := codec.HashKey(key)
	if !ms.keySet.Has(h) {
		return nil, false
	}

	var value []byte
	var found bool
	ms.dataBytes.ForEach(func(b []byte) bool {
		flag, k, v, err := codec.DecodeRecord(b)
		if err != nil {
			return true
		}

		if flag == codec.FlagTombstone {
			return true
		}
		if flag != codec.FlagDel && bytes.Equal(k, key) {
			value = v
			found = true
			return true
		}
		return false
	})

	return value, found
}

const initRef = 1

// diskSegment 磁盘分段 持有一个顺序写入的磁盘块的所有数据信息
//
// diskSegment 采用引用计数的方式来提高并发读写能力 即 ref == 0 时 gc 线程会负责将其清理
// 每次有读操作时 ref++ 操作完毕后 ref--
type diskSegment struct {
	seqID     int64
	ref       int64
	size      int
	path      string
	dataCfd   *fsx.CacheFD
	keysCfd   *fsx.CacheFD
	bf        uint64set.BloomFilter
	pos       codec.BucketPos
	clearAtFn func(string) int64
}

type diskSegments []*diskSegment

// OrderIncrement 正序排序 disk segment list
func (dss diskSegments) OrderIncrement() {
	sort.Slice(dss, func(i, j int) bool {
		return dss[i].seqID < dss[j].seqID
	})
}

// OrderDecrement 倒序排序 disk segment list
func (dss diskSegments) OrderDecrement() {
	sort.Slice(dss, func(i, j int) bool {
		return dss[i].seqID > dss[j].seqID
	})
}

func newDiskSegment(seqID int64, size int, path string, bf uint64set.BloomFilter, meta codec.BucketPos) *diskSegment {
	return &diskSegment{
		ref:   initRef,
		seqID: seqID,
		size:  size,
		path:  path,
		bf:    bf,
		pos:   meta,
	}
}

func (ds *diskSegment) loadRef() int64 {
	return atomic.LoadInt64(&ds.ref)
}

func (ds *diskSegment) incRef() {
	atomic.AddInt64(&ds.ref, 1)
}

func (ds *diskSegment) decRef() {
	if n := atomic.AddInt64(&ds.ref, -1); n < 0 {
		panic("BUG: ref must not be negative")
	}
}

// Install 装载 disk segment 移除 tmp 文件并生成 fd 缓存
func (ds *diskSegment) Install() error {
	var errs []error

	keysFile := fsx.KeysFilename(ds.seqID, ds.path)
	keysTemp := fsx.KeysTmpFilename(ds.seqID, ds.path)
	if err := os.Rename(keysTemp, keysFile); err != nil {
		errs = append(errs, err)
	}

	dataFile := fsx.DataFilename(ds.seqID, ds.path)
	dataTemp := fsx.DataTmpFilename(ds.seqID, ds.path)
	if err := os.Rename(dataTemp, dataFile); err != nil {
		errs = append(errs, err)
	}

	var err error
	if ds.dataCfd == nil {
		if ds.dataCfd, err = fsx.NewCacheFD(dataFile, dataMaxFdHold); err != nil {
			errs = append(errs, err)
		}
	}
	if ds.keysCfd == nil {
		if ds.keysCfd, err = fsx.NewCacheFD(keysFile, keysMaxFdHold); err != nil {
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

// Close 关闭持有的 fd
func (ds *diskSegment) Close() error {
	return ds.dataCfd.Close()
}

// Remove 删除磁盘文件
func (ds *diskSegment) Remove() error {
	return multierr.Combine(
		os.Remove(fsx.DataFilename(ds.seqID, ds.path)),
		os.Remove(fsx.KeysFilename(ds.seqID, ds.path)),
	)
}

// Get 返回指定 Key 对应的 Value
func (ds *diskSegment) Get(name string, key []byte) ([]byte, error) {
	ds.incRef()
	defer ds.decRef()

	// bucket 执行过 clear 不再检索
	if ds.clearAtFn(name) > ds.seqID {
		return nil, nil
	}

	// 该 seg 种不存在制定 bucket
	positions, ok := ds.pos.Record[name]
	if !ok {
		return nil, nil
	}

	// bloomfilter 判定
	h := codec.HashKey(key)
	if !ds.bf.Test(h) {
		return nil, nil
	}

	excepted := key
	reader, err := ds.dataCfd.FileDesc()
	if err != nil {
		return nil, err
	}
	defer ds.dataCfd.Reuse(reader)

	b := make([]byte, positions.MaxRange())
	var value []byte
	var found bool

	for _, position := range positions {
		l := position.End - position.Start
		n, err := reader.ReadAt(b[:l], int64(position.Start))
		if err != nil {
			return nil, err
		}
		if n != int(l) {
			return nil, codec.ErrReadPartial
		}

		encoded, err := codec.VerifyTailChecksum(b[:n])
		if err != nil {
			return nil, err
		}

		rg := codec.NewRecordRanger(bytes.NewReader(encoded))
		err = rg.Range(func(flag codec.Flag, key, val []byte, n int) bool {
			if bytes.Equal(excepted, key) {
				value = val
				found = true
			}
			return found
		})

		if err != nil {
			return nil, err
		}

		if found {
			break
		}
	}
	return value, nil
}

// Range 遍历每个 Key 并执行 fn 方法
// pass 用于判断是否跳出 range 循环
func (ds *diskSegment) Range(name string, visitFn codec.RecordVisitFunc, passFn codec.PassFunc) error {
	ds.incRef()
	defer ds.decRef()

	if ds.clearAtFn(name) > ds.seqID {
		return nil
	}
	positions, ok := ds.pos.Record[name]
	if !ok {
		return nil
	}

	reader, err := ds.dataCfd.FileDesc()
	if err != nil {
		return err
	}
	defer ds.dataCfd.Reuse(reader)

	b := make([]byte, positions.MaxRange())
	for _, position := range positions {
		l := position.End - position.Start
		n, err := reader.ReadAt(b[:l], int64(position.Start))
		if err != nil {
			return err
		}
		if n != int(l) {
			return codec.ErrReadPartial
		}

		encoded, err := codec.VerifyTailChecksum(b[:n])
		if err != nil {
			return err
		}

		rg := codec.NewRecordRanger(bytes.NewReader(encoded))
		err = rg.Range(func(flag codec.Flag, key, val []byte, n int) bool {
			if passFn(flag, codec.HashKey(key)) {
				visitFn(flag, key, val, n)
			}
			return false
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// rangeKeys 只有在 Compact 过程中使用到 需要遍历每一个 key
func (ds *diskSegment) rangeKeys(name string, visitFn codec.KeyVisitFunc) error {
	ds.incRef()
	defer ds.decRef()

	positions, ok := ds.pos.KeyItem[name]
	if !ok {
		return nil
	}

	reader, err := ds.keysCfd.FileDesc()
	if err != nil {
		return err
	}
	defer ds.keysCfd.Reuse(reader)

	b := make([]byte, positions.MaxRange())
	for _, position := range positions {
		l := position.End - position.Start
		n, err := reader.ReadAt(b[:l], int64(position.Start))
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if n != int(l) {
			return codec.ErrReadPartial
		}

		encoded, err := codec.VerifyTailChecksum(b[:n])
		if err != nil {
			return err
		}

		rg := codec.NewKeysRanger(bytes.NewReader(encoded))
		err = rg.Range(func(flag codec.Flag, h uint64, n uint32) {
			visitFn(flag, h, n)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type iterReleaser struct {
	segs []*diskSegment
}

func newDiskSegmentVersion(segs []*diskSegment) *iterReleaser {
	return &iterReleaser{segs: segs}
}

func (dsv *iterReleaser) iter(fn func(*diskSegment) bool) {
	for _, seg := range dsv.segs {
		quit := fn(seg)
		if quit {
			return
		}
	}
}

func (dsv *iterReleaser) release() {
	for _, seg := range dsv.segs {
		seg.decRef()
	}
}
