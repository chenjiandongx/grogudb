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
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chenjiandongx/grogudb/pkg/buffer"
	"github.com/chenjiandongx/grogudb/pkg/codec"
	"github.com/chenjiandongx/grogudb/pkg/fsx"
	"github.com/chenjiandongx/grogudb/pkg/logx"
	"github.com/chenjiandongx/grogudb/pkg/uint64set"
)

const chunkSize = 1024 * 1024 * 4

// compactionPlan compact 计划 描述将 source disk segments 合并至 destination segment
type compactionPlan struct {
	src []*diskSegment
	dst *diskSegment
}

// String 格式化 compactionPlan 输出
func (c compactionPlan) String() string {
	if len(c.src) <= 0 || c.dst == nil {
		return ""
	}

	var srcSeqIDs []string
	for _, ds := range c.src {
		srcSeqIDs = append(srcSeqIDs, strconv.Itoa(int(ds.seqID)))
	}
	return fmt.Sprintf("src.seqID=[%v] -> dst.seqID=[%d]", strings.Join(srcSeqIDs, ","), c.dst.seqID)
}

func (db *DB) getBucketClearAt(name string) int64 {
	db.memMut.Lock()
	defer db.memMut.Unlock()

	v, ok := db.buckets[name]
	if !ok {
		return 0
	}
	return v.head.clearAt
}

// splitDiskSegmentGroups 将传入的 segs 切分为多个分组
func (db *DB) splitDiskSegmentGroups(segs diskSegments) []diskSegments {
	count := len(segs)
	groups := make([]diskSegments, 0)
	group := make(diskSegments, 0)

	var idx int
	var size int
	for {
		if idx >= count {
			break
		}

		size += segs[idx].size
		group = append(group, segs[idx])

		// 当 group 中文件大小超过 db.opts.MaxDiskSegmentBytes 时切分为一个分组
		if size > db.opts.MaxDiskSegmentBytes {
			// 补偿回退 不然会丢弃当次循环的 seg
			if len(group) > 1 {
				idx--
				group = group[:len(group)-1]
			}

			// 追加至 groups 中并置空状态
			size = 0
			groups = append(groups, group)
			group = make(diskSegments, 0)
		}
		idx++
	}

	if len(group) > 0 {
		groups = append(groups, group)
	}

	return groups
}

// getCompactionPlans 生成 compaction plans
func (db *DB) getCompactionPlans() ([]compactionPlan, error) {
	db.diskMut.Lock()
	segs := make(diskSegments, 0, len(db.diskSegs))
	for i := range db.diskSegs {
		seg := db.diskSegs[i]
		seg.incRef()
		segs = append(segs, seg)
	}
	segs.OrderIncrement() // 时间序
	db.diskMut.Unlock()

	// 退出前保证 desc reference
	defer func() {
		for _, seg := range segs {
			seg.decRef()
		}
	}()

	var plans []compactionPlan
	groups := db.splitDiskSegmentGroups(segs)
	for _, group := range groups {
		cmp := newCompactor(db.opts.CompactFragmentation, db.getBucketClearAt, db.bucketsKeys.HasKey)

		// 分组长度为 1 时先判断是否需要 Compact
		if len(group) == 1 {
			needed, err := cmp.needCompat(group[0])
			if err != nil {
				return nil, err
			}
			if !needed {
				continue
			}
		}

		// 其余情况可以合并多个 segs
		group.OrderDecrement()
		newSeg, err := cmp.doCompact(group...)
		if err != nil {
			return nil, err
		}
		plans = append(plans, compactionPlan{
			src: group,
			dst: newSeg,
		})
	}

	return plans, nil
}

// compactor 负责判断并压缩 disk segments
type compactor struct {
	fragmentation float64
	onExist       func(name string, h uint64) bool
	bucketPos     codec.BucketPos
	clearAtFn     func(string) int64
	visited       *uint64set.Sets
	deleted       *uint64set.Sets

	dataStart uint32
	dataEnd   uint32
	keysStart uint32
	keysEnd   uint32
}

// newCompactor 生成并返回 *compactor 实例
func newCompactor(fragmentation float64, clearAtFn func(string) int64, onExist func(string, uint64) bool) *compactor {
	return &compactor{
		fragmentation: fragmentation,
		onExist:       onExist,
		visited:       uint64set.NewSets(),
		deleted:       uint64set.NewSets(),
		clearAtFn:     clearAtFn,
		bucketPos: codec.BucketPos{
			Record:  map[string]codec.Positions{},
			KeyItem: map[string]codec.Positions{},
		},
	}
}

// mergeBucketNames 合并多个 disk segments 的 bucket names 并进行排序
func mergeBucketNames(diskSegs ...*diskSegment) []string {
	unique := map[string]struct{}{}
	for _, diskSeg := range diskSegs {
		for name := range diskSeg.pos.Record {
			unique[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// needCompat 判断是否需要 compact
func (c *compactor) needCompat(diskSeg *diskSegment) (bool, error) {
	var deletedBytes int
	for bucket := range diskSeg.pos.KeyItem {
		// 如果 bucket Clear 的时间大于 disk segment 构建时间 则需要清除所有 keys
		if c.clearAtFn(bucket) > diskSeg.seqID {
			err := diskSeg.rangeKeys(bucket, func(flag codec.Flag, h uint64, n uint32) {
				deletedBytes += int(n)
			})
			if err != nil {
				return false, err
			}
			continue
		}

		visited := uint64set.NewSet()
		deleted := uint64set.NewSet()
		err := diskSeg.rangeKeys(bucket, func(flag codec.Flag, h uint64, n uint32) {
			// 如果 key 在最新状态的 bucket 中不存在 则删除
			if !c.onExist(bucket, h) {
				deletedBytes += int(n)
				return
			}

			// 如果该 key 已删除 则后续相同的 key 都不再保留
			if flag == codec.FlagDel && !deleted.Has(h) {
				deleted.Insert(h)
			}
			if deleted.Has(h) {
				deletedBytes += int(n)
				return
			}

			// FlagPut: 取最新的 Record
			if !visited.Has(h) {
				visited.Insert(h)
				return
			}
			deletedBytes += int(n)
		})
		if err != nil {
			return false, err
		}
	}

	// 当 deletedBytes 超过一定比例时才具备 Compact 效益
	return float64(diskSeg.size)*c.fragmentation < float64(deletedBytes), nil
}

// doCompact 执行真正的 compact 逻辑
func (c *compactor) doCompact(diskSegs ...*diskSegment) (*diskSegment, error) {
	last := len(diskSegs) - 1 // 往更大的 seqID 靠近

	seqID := diskSegs[last].seqID + 1 // 保证文件名不重复
	path := diskSegs[last].path

	dataTemp := fsx.DataTmpFilename(seqID, path)
	keysTemp := fsx.KeysTmpFilename(seqID, path)

	var err error
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dataTemp)
			_ = os.RemoveAll(keysTemp)
		}
	}()

	var dataF *os.File
	if dataF, err = os.OpenFile(dataTemp, fsx.FlagAppend, 0o644); err != nil {
		return nil, err
	}
	defer dataF.Close()

	var keysF *os.File
	if keysF, err = os.OpenFile(keysTemp, fsx.FlagAppend, 0o644); err != nil {
		return nil, err
	}
	defer keysF.Close()

	buckets := mergeBucketNames(diskSegs...)
LOOP:
	for _, bucket := range buckets {
		for _, diskSeg := range diskSegs {
			if err = c.compact(dataF, keysF, bucket, diskSeg); err != nil {
				break LOOP
			}
		}
	}

	// 判断 Compact 是否出错
	if err != nil {
		return nil, err
	}

	metaBytes := codec.EncodeMetadata(c.bucketPos.AsBucketMetaSlice())
	_, err = dataF.Write(metaBytes)
	if err != nil {
		return nil, err
	}

	bf := c.visited.AsBloomFilter()
	bfBytes := bf.Bytes()
	if _, err := dataF.Write(bfBytes); err != nil {
		return nil, err
	}

	footer := codec.Footer{
		DataSize:         c.dataStart,
		MetaSize:         uint32(len(metaBytes)),
		BloomFilterCount: uint32(bf.Count()),
		BloomFilterSize:  uint32(len(bfBytes)),
	}

	_, err = dataF.Write(codec.EncodeFooter(footer))
	if err != nil {
		return nil, err
	}

	diskSeg := newDiskSegment(seqID, int(c.dataEnd), path, bf, c.bucketPos)
	diskSeg.clearAtFn = c.clearAtFn
	return diskSeg, nil
}

func (c *compactor) compact(dw, kw io.Writer, bucket string, diskSeg *diskSegment) error {
	dataBuf := buffer.Get()
	keysBuf := buffer.Get()
	defer func() {
		buffer.Put(dataBuf)
		buffer.Put(keysBuf)
	}()

	var err error
	visited := c.visited.GetOrCreate(bucket)
	deleted := c.deleted.GetOrCreate(bucket)
	err = diskSeg.Range(bucket, func(flag codec.Flag, key, val []byte, n int) bool {
		// 若 bucket 执行了 Clear 则优先判断是否丢弃
		if c.clearAtFn(bucket) > diskSeg.seqID {
			return true
		}

		h := codec.HashKey(key)
		if !c.onExist(bucket, h) {
			return false
		}

		// 如果该 key 已删除 则后续相同的 key 都不再保留
		if flag == codec.FlagDel && !deleted.Has(h) {
			deleted.Insert(h)
		}
		if deleted.Has(h) {
			return false
		}

		if visited.Has(h) {
			return false
		}
		visited.Insert(h)

		b := codec.EncodeRecord(codec.FlagPut, key, val)
		_, _ = dataBuf.Write(b)
		c.dataEnd += uint32(len(b))

		b = codec.EncodeKeyEntity(codec.FlagPut, h, uint32(len(b)))
		_, _ = keysBuf.Write(b)
		c.keysEnd += uint32(len(b))

		if dataBuf.Len() >= chunkSize {
			c.dataEnd += codec.SizeChecksum
			if _, err = dw.Write(dataBuf.Frozen()); err != nil {
				return true
			}

			c.keysEnd += codec.SizeChecksum
			if _, err = kw.Write(keysBuf.Frozen()); err != nil {
				return true
			}

			c.bucketPos.Record[bucket] = append(c.bucketPos.Record[bucket], codec.Position{
				Start: c.dataStart,
				End:   c.dataEnd,
			})
			c.dataStart = c.dataEnd

			c.bucketPos.KeyItem[bucket] = append(c.bucketPos.KeyItem[bucket], codec.Position{
				Start: c.keysStart,
				End:   c.keysEnd,
			})
			c.keysStart = c.keysEnd

			// 置空 buffer 但不回收
			dataBuf.Reset()
			keysBuf.Reset()
		}
		return false
	}, codec.PassAll())

	if err != nil {
		return err
	}

	if dataBuf.Len() > 0 {
		c.dataEnd += codec.SizeChecksum
		if _, err = dw.Write(dataBuf.Frozen()); err != nil {
			return err
		}

		c.keysEnd += codec.SizeChecksum
		if _, err = kw.Write(keysBuf.Frozen()); err != nil {
			return err
		}

		c.bucketPos.Record[bucket] = append(c.bucketPos.Record[bucket], codec.Position{
			Start: c.dataStart,
			End:   c.dataEnd,
		})
		c.dataStart = c.dataEnd

		c.bucketPos.KeyItem[bucket] = append(c.bucketPos.KeyItem[bucket], codec.Position{
			Start: c.keysStart,
			End:   c.keysEnd,
		})
		c.keysStart = c.keysEnd
	}

	return nil
}

// removeDiskSegments 移除给定 disk segments
// 调用方需保证线程安全 此处不加锁
func (db *DB) removeDiskSegments(segs ...*diskSegment) {
	count := len(segs)
	var n int
	for {
		if n >= count {
			break
		}

		var index int
		for i, seg := range db.diskSegs {
			if seg.seqID == segs[n].seqID {
				index = i
				break
			}
		}

		db.diskSegs[index].decRef()
		db.garbageDiskSegs = append(db.garbageDiskSegs, db.diskSegs[index])
		db.diskSegs = append(db.diskSegs[:index], db.diskSegs[index+1:]...)
		n++
	}
}

// Compact 合并 disk segment 减少磁盘空间占用
// 同一时刻只能有一次 Compact 操作 执行操作返回 true 反之返回 false
func (db *DB) Compact() (bool, error) {
	var compacted bool
	if !db.state.compacting.CompareAndSwap(false, true) {
		return compacted, nil
	}

	defer db.state.compacting.Store(false)
	compacted = true

	start := time.Now()
	plans, err := db.getCompactionPlans()
	if err != nil {
		return compacted, err
	}

	// 没有 compaction 计划 提前返回
	if len(plans) <= 0 {
		return compacted, nil
	}

	db.diskMut.Lock()
	defer db.diskMut.Unlock()

	for _, plan := range plans {
		logx.Infof("get compaction plan %s", plan)
		db.removeDiskSegments(plan.src...)
		if err := plan.dst.Install(); err != nil {
			logx.Errorf("install segment failed, SeqID·%d, err=%v", plan.dst.seqID, err)
			continue
		}

		db.diskSegs = append(db.diskSegs, plan.dst)
		db.diskSegs.OrderDecrement()
		db.stats.diskSegment.Add(1)
		db.stats.compact.Add(1)
	}
	logx.Infof("compaction operation elapsed %v", time.Since(start))
	return compacted, nil
}

func (db *DB) evaluateCompact(prev, curr Stats) bool {
	// 如果进程正在 rotating 中 不进行操作
	if db.state.rotating.Load() {
		return false
	}

	// 如果存在清除 Bucket 操作 需要进行 compact
	if curr.Clear-prev.Clear > 0 {
		return true
	}

	// keyOp 均没有超过 CompactKeyOpDelta 需要跳过
	if int(curr.Del-prev.Del) <= db.opts.CompactKeyOpDelta && int(curr.Put-prev.Put) <= db.opts.CompactKeyOpDelta {
		return false
	}

	return true
}

func (db *DB) loopCompact() {
	logx.Infof("start compaction worker")

	db.waiting.Inc()
	defer db.waiting.Dec()

	ticker := time.NewTicker(db.opts.CompactCheckInterval)
	defer ticker.Stop()

	statsCache := db.Stats()
	last := time.Now()

	for {
		select {
		case <-db.ctx.Done():
			return

		case <-ticker.C:
			now := time.Now()
			stats := db.Stats()
			goahead := db.evaluateCompact(statsCache, stats)
			force := now.Second()-last.Second() > int(db.opts.CompactForceInterval.Seconds())
			if !goahead && !force {
				continue
			}

			last = now
			statsCache = stats // 跨周期对比
			if _, err := db.Compact(); err != nil {
				logx.Errorf("compaction failed, err=%v", err)
			}
		}
	}
}
