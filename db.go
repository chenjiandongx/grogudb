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
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/juju/fslock"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/chenjiandongx/grogudb/pkg/buffer"
	"github.com/chenjiandongx/grogudb/pkg/codec"
	"github.com/chenjiandongx/grogudb/pkg/fsx"
	"github.com/chenjiandongx/grogudb/pkg/logx"
	"github.com/chenjiandongx/grogudb/pkg/uint64set"
	"github.com/chenjiandongx/grogudb/pkg/wait"
)

const (
	lockName = "_lock"

	keysMaxFdHold = 1
	dataMaxFdHold = 4
)

type operation uint8

const (
	opPut operation = iota
	opPutIf
	opDel
)

var (
	ErrEmptyDBPath = errors.New("grogudb: empty database path")
	ErrClosed      = errors.New("grogudb: database closed")
)

// Bytes 定义为 []byte 提供了 Copy 方法
type Bytes []byte

// Copy 复制 bytes
func (b Bytes) Copy() []byte {
	if b == nil {
		return nil
	}
	if len(b) == 0 {
		return []byte{}
	}

	dst := make([]byte, len(b))
	copy(dst, b)
	return dst
}

// B 将 Bytes 转换为 []byte
func (b Bytes) B() []byte {
	return b
}

// Nil 返回 Bytes 是否为 nil
func (b Bytes) Nil() bool {
	return b == nil
}

// String 将 Bytes 转换为 string
func (b Bytes) String() string {
	return string(b)
}

// DB grogudb 实例实现
//
// 管控着所有的 db 操作行为以及 db 的状态管理
type DB struct {
	opts *Options

	ctx    context.Context
	cancel context.CancelFunc
	flock  *fslock.Lock

	bucketsKeys *uint64set.Sets
	markBytesCh chan int64
	path        string
	clearAt     map[string]int64

	memMut     sync.RWMutex
	buckets    map[string]*Bucket
	memorySegs map[string]*memorySegment

	diskMut         sync.Mutex
	diskSegs        diskSegments
	garbageDiskSegs diskSegments // 待清理的 disk segment

	stats   *statistics
	state   *state
	waiting wait.Waiting
}

// Open 打开一个 db 实例 打开 db 链接时会持有文件锁 避免二次打开
func Open(path string, opts *Options) (*DB, error) {
	if path == "" {
		return nil, ErrEmptyDBPath
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if opts == nil {
		opts = &Options{}
	}
	opts.Validate()
	logx.Infof("database options: %+v", *opts)

	flock := fslock.New(filepath.Join(path, lockName))
	if err := flock.TryLock(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{
		ctx:         ctx,
		cancel:      cancel,
		flock:       flock,
		path:        path,
		opts:        opts,
		bucketsKeys: uint64set.NewSets(),
		buckets:     make(map[string]*Bucket),
		memorySegs:  make(map[string]*memorySegment),
		markBytesCh: make(chan int64, 1),
		stats:       &statistics{},
		state:       &state{},
		clearAt:     make(map[string]int64),
	}

	if err := db.loadDiskSegments(); err != nil {
		return nil, err
	}

	go wait.Until(ctx, db.loopRotate)
	go wait.Until(ctx, db.loopCompact)
	go wait.Until(ctx, db.loopGc)
	db.waiting.Until(3)

	return db, nil
}

func (db *DB) loopGc() {
	logx.Infof("start gc worker")

	db.waiting.Inc()
	defer db.waiting.Dec()

	ticker := time.NewTicker(db.opts.GcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return

		case <-ticker.C:
			db.Gc()
		}
	}
}

// Gc 负责清理 handing 状态的 disk segment
// 此 API 用户一般无需手动调用
func (db *DB) Gc() {
	db.diskMut.Lock()
	defer db.diskMut.Unlock()

	var changed bool
	for i := range db.garbageDiskSegs {
		seg := db.garbageDiskSegs[i]
		if seg.loadRef() > 0 {
			continue
		}

		changed = true
		db.stats.diskSegment.Add(-1)
		if err := seg.Remove(); err != nil {
			logx.Errorf("remove segment failed, SegID·%d, err=%v", seg.seqID, err)
			continue
		}
		logx.Infof("gc segment, SegID·%d", seg.seqID)
		db.garbageDiskSegs[i] = nil // 释放资源
	}

	if !changed {
		return
	}

	nextRound := make(diskSegments, 0)
	for i := range db.garbageDiskSegs {
		if seg := db.garbageDiskSegs[i]; seg != nil {
			nextRound = append(nextRound, seg)
		}
	}
	db.garbageDiskSegs = nextRound
}

func (db *DB) loopRotate() {
	logx.Infof("start rotation worker")

	db.waiting.Inc()
	defer db.waiting.Dec()

	for {
		select {
		case <-db.ctx.Done():
			if err := db.rotate(); err != nil {
				logx.Errorf("rotate segment failed, err=%v", err)
			}
			return

		case n := <-db.markBytesCh:
			cur := db.stats.memHoldBytes.Add(n)
			if cur < int64(db.opts.MaxMemSegmentBytes) {
				continue
			}
			if err := db.rotate(); err != nil {
				logx.Errorf("rotate segment failed, err=%v", err)
			}
		}
	}
}

func (db *DB) getMemoryBuckets() []string {
	db.memMut.RLock()
	defer db.memMut.RUnlock()

	names := make([]string, 0, len(db.memorySegs))
	for name, seg := range db.memorySegs {
		if seg.Len() > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (db *DB) rotate() error {
	if !db.state.rotating.CompareAndSwap(false, true) {
		return nil
	}

	defer db.state.rotating.Store(false)
	db.stats.rotate.Add(1)

	names := db.getMemoryBuckets()
	if len(names) <= 0 {
		return nil
	}

	dataBuf := buffer.Get()
	keysBuf := buffer.Get()
	defer func() {
		buffer.Put(dataBuf)
		buffer.Put(keysBuf)
	}()

	var bms codec.BucketMetaSlice
	var dataStart, keysStart int
	keySets := uint64set.NewSets()

	var total int
	for _, name := range names {
		db.memMut.Lock()
		seg := db.memorySegs[name]
		dataBytes, keysBytes, keySet := seg.Flush()
		db.memMut.Unlock()

		l := len(dataBytes) + len(keysBytes)
		if l <= 0 {
			continue
		}
		keySets.Update(name, keySet)

		total += len(dataBytes)
		total -= codec.SizeChecksum
		bms = append(bms, codec.BucketMeta{
			Name: name,
			RecordPos: []codec.Position{{
				Start: uint32(dataStart),
				End:   uint32(dataStart + len(dataBytes)),
			}},
			KeyEntityPos: []codec.Position{{
				Start: uint32(keysStart),
				End:   uint32(keysStart + len(keysBytes)),
			}},
		})

		dataStart += len(dataBytes)
		keysStart += len(keysBytes)
		_, _ = dataBuf.Write(dataBytes)
		_, _ = keysBuf.Write(keysBytes)
	}

	db.stats.memHoldBytes.Add(int64(-total))
	if dataBuf.Len() <= 0 || keysBuf.Len() <= 0 {
		return nil
	}

	// 使用 UnixNano 作为 seqID 保证单调递增
	seqID := time.Now().UnixNano()

	keysPath := fsx.KeysFilename(seqID, db.path)
	dataPath := fsx.DataFilename(seqID, db.path)

	var err error
	defer func() {
		if err != nil {
			_ = os.RemoveAll(keysPath)
			_ = os.RemoveAll(dataPath)
		}
	}()

	if err = fsx.WriteFile(keysPath, keysBuf.Bytes()); err != nil {
		return err
	}

	var dataF *os.File
	dataF, err = os.OpenFile(dataPath, fsx.FlagAppend, 0o644)
	if err != nil {
		return err
	}
	defer dataF.Close()

	footer := codec.Footer{}
	footer.DataSize = uint32(dataBuf.Len())
	if _, err = dataF.Write(dataBuf.Bytes()); err != nil {
		return err
	}

	metaBytes := codec.EncodeMetadata(bms)
	footer.MetaSize = uint32(len(metaBytes))
	if _, err = dataF.Write(metaBytes); err != nil {
		return err
	}

	bf := keySets.AsBloomFilter()
	bfBytes := bf.Bytes()
	footer.BloomFilterCount = uint32(keySets.CountAll())
	footer.BloomFilterSize = uint32(len(bf.Bytes()))
	if _, err = dataF.Write(bfBytes); err != nil {
		return err
	}

	footerBytes := codec.EncodeFooter(footer)
	if _, err = dataF.Write(footerBytes); err != nil {
		return err
	}

	diskSeg := newDiskSegment(seqID, dataBuf.Len(), db.path, bf, bms.AsBucketPos())
	diskSeg.clearAtFn = db.getBucketClearAt

	diskSeg.dataCfd, err = fsx.NewCacheFD(dataPath, dataMaxFdHold)
	if err != nil {
		return err
	}
	diskSeg.keysCfd, err = fsx.NewCacheFD(keysPath, keysMaxFdHold)
	if err != nil {
		return err
	}

	db.diskMut.Lock()
	db.diskSegs = append(db.diskSegs, diskSeg)
	db.stats.diskSegment.Add(1)
	db.diskSegs.OrderDecrement()
	db.diskMut.Unlock()

	logx.Infof("rotate KeyF·%s, DataF·%s", fsx.KeysFilename(seqID), fsx.DataFilename(seqID))

	return nil
}

// Buckets 返回当前 db 所有 buckets 名称
func (db *DB) Buckets() []string {
	db.memMut.Lock()
	defer db.memMut.Unlock()

	buckets := make([]string, 0, len(db.buckets))
	for k := range db.buckets {
		buckets = append(buckets, k)
	}
	sort.Strings(buckets)
	return buckets
}

func (db *DB) getIterReleaser() *iterReleaser {
	db.diskMut.Lock()
	segs := make([]*diskSegment, 0, len(db.diskSegs))
	for _, seg := range db.diskSegs {
		seg.incRef()
		segs = append(segs, seg)
	}
	db.diskMut.Unlock()

	return newDiskSegmentVersion(segs)
}

// Stats 返回 db 操作统计情况
func (db *DB) Stats() Stats {
	return db.stats.Load()
}

// State 返回 db 状态
func (db *DB) State() State {
	return db.state.Load()
}

// GetOrCreateBucket 获取或创建 Bucket 实例
func (db *DB) GetOrCreateBucket(name string) *Bucket {
	db.memMut.RLock()
	seg := db.memorySegs[name]
	db.memMut.RUnlock()

	if seg != nil {
		return db.buckets[name]
	}

	db.memMut.Lock()
	defer db.memMut.Unlock()

	seg = db.memorySegs[name]
	if seg != nil {
		return db.buckets[name]
	}

	keySet := db.bucketsKeys.GetOrCreate(name)
	seg = newMemorySegment(name, memSegCallback{
		onRemove: keySet.Remove,
		onInsert: keySet.Insert,
		onExist:  keySet.Has,
		onBytes: func(n int64) {
			db.markBytesCh <- n
		},
	})
	db.stats.memSegment.Add(1)
	db.memorySegs[name] = seg

	bucket := &Bucket{
		name:            name,
		keys:            keySet,
		head:            seg,
		statistics:      db.stats,
		getIterReleaser: db.getIterReleaser,
	}
	db.buckets[name] = bucket
	return bucket
}

// Close 关闭 db
func (db *DB) Close() error {
	if !db.state.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	db.cancel()
	db.diskMut.Lock()
	var errs []error
	for _, seg := range db.diskSegs {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	db.diskMut.Unlock()
	db.waiting.Until(0)

	if err := db.flock.Unlock(); err != nil {
		errs = append(errs, err)
	}

	return multierr.Combine(errs...)
}
