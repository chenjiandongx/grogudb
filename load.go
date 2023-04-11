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
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/chenjiandongx/grogudb/pkg/codec"
	"github.com/chenjiandongx/grogudb/pkg/fsx"
	"github.com/chenjiandongx/grogudb/pkg/logx"
	"github.com/chenjiandongx/grogudb/pkg/uint64set"
)

type segmentFile struct {
	seqID    int64
	keysFile os.DirEntry
	dataFile os.DirEntry
}

// splitEntries 切分 segmentFiles 相同 seqID keysFile/dataFile 为一组
func splitEntries(entries []os.DirEntry) []segmentFile {
	unique := make(map[string]os.DirEntry)
	for _, entry := range entries {
		// 递归文件夹不处理
		if entry.IsDir() {
			continue
		}
		unique[entry.Name()] = entry
	}

	segmentFiles := make(map[int64]*segmentFile)
	for name, entry := range unique {
		// 非法文件名不做处理
		prefix, seqID, ok := fsx.ParseFilename(name)
		if !ok {
			continue
		}

		if prefix == fsx.PrefixDataFile {
			segmentFiles[seqID] = &segmentFile{
				seqID:    seqID,
				dataFile: entry,
			}
		}
	}

	for seqID, sf := range segmentFiles {
		keysFile, ok := unique[fsx.KeysFilename(seqID)]
		if !ok {
			delete(segmentFiles, seqID)
			continue
		}

		sf.keysFile = keysFile
	}

	var ret []segmentFile
	for _, sf := range segmentFiles {
		ret = append(ret, *sf)
	}

	// 按时间顺序排序
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].seqID < ret[j].seqID
	})
	return ret
}

// loadFooter 加载 Footer
func loadFooter(cfd *fsx.CacheFD) (codec.Footer, error) {
	var footer codec.Footer
	fd, err := cfd.FileDesc()
	if err != nil {
		return footer, err
	}
	defer cfd.Reuse(fd)

	offset := cfd.Size() - codec.FooterSize
	if offset < 0 {
		return footer, codec.ErrInvalidFooterSize
	}

	b := make([]byte, codec.FooterSize)
	_, err = fd.ReadAt(b, offset)
	if err != nil {
		return footer, err
	}

	return codec.DecodeFooter(b)
}

// loadMetadata 加载 metadata
func loadMetadata(cfd *fsx.CacheFD, footer codec.Footer) (codec.BucketMetaSlice, error) {
	fd, err := cfd.FileDesc()
	if err != nil {
		return nil, err
	}
	defer cfd.Reuse(fd)

	b := make([]byte, footer.MetaSize)
	_, err = fd.ReadAt(b, int64(footer.PosMetaBlock().Start))
	if err != nil {
		return nil, err
	}

	return codec.DecodeMetadata(b)
}

// loadBloomFilter 加载 bloomfilter
func loadBloomFilter(cfd *fsx.CacheFD, footer codec.Footer) (uint64set.BloomFilter, error) {
	fd, err := cfd.FileDesc()
	if err != nil {
		return nil, err
	}
	defer cfd.Reuse(fd)

	b := make([]byte, footer.BloomFilterSize)
	_, err = fd.ReadAt(b, int64(footer.PosBloomFilterBlock().Start))
	if err != nil {
		return nil, err
	}

	return uint64set.LoadBloomFilter(int(footer.BloomFilterCount), b)
}

func (db *DB) loadDiskSegments() error {
	start := time.Now()
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return err
	}
	segmentFiles := splitEntries(entries)
	if len(segmentFiles) <= 0 {
		return nil
	}

	for _, sf := range segmentFiles {
		logx.Infof("load segment KeyF·%s, DataF·%s", sf.keysFile.Name(), sf.dataFile.Name())
		if err := db.loadDiskSegment(db.path, sf); err != nil {
			return err
		}
	}

	// 如果 bucket 中没有任何 key 则表示该 bucket 在此次加载中将被丢弃
	var dropBuckets []string
	for name, bucket := range db.buckets {
		if bucket.keys.Count() <= 0 {
			dropBuckets = append(dropBuckets, name)
		}
	}
	for _, name := range dropBuckets {
		// 清理内存记录
		delete(db.memorySegs, name)
		db.stats.memSegment.Add(-1)

		// 清理 buckets 记录
		delete(db.buckets, name)
		db.bucketsKeys.Remove(name)
		logx.Infof("drop bucket %s", name)
	}

	logx.Infof("load %d segments elapsed %v", len(db.diskSegs), time.Since(start))
	return nil
}

// loadKeys 加载 keys
func (db *DB) loadKeys(cfd *fsx.CacheFD, bucketPos codec.BucketPos, seqID int64) error {
	fd, err := cfd.FileDesc()
	if err != nil {
		return err
	}

	for bucket, positions := range bucketPos.KeyItem {
		bucketKeys := db.bucketsKeys.GetOrCreate(bucket)
		for _, pos := range positions {
			b := make([]byte, pos.End-pos.Start)
			_, err = fd.ReadAt(b, int64(pos.Start))
			if err != nil {
				return err
			}

			encoded, err := codec.VerifyTailChecksum(b)
			if err != nil {
				return err
			}

			rg := codec.NewKeysRanger(bytes.NewReader(encoded))
			err = rg.Range(func(flag codec.Flag, h uint64, n uint32) {
				switch flag {
				case codec.FlagDel:
					bucketKeys.Remove(h)
				case codec.FlagTombstone:
					bucketKeys.Clear()
					db.clearAt[bucket] = seqID
				default:
					bucketKeys.Insert(h)
				}
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *DB) loadDiskSegment(path string, sf segmentFile) error {
	dataPath := filepath.Join(path, sf.dataFile.Name())
	dataCfd, err := fsx.NewCacheFD(dataPath, dataMaxFdHold)
	if err != nil {
		return err
	}

	footer, err := loadFooter(dataCfd)
	if err != nil {
		return err
	}

	bms, err := loadMetadata(dataCfd, footer)
	if err != nil {
		return err
	}
	bucketPos := bms.AsBucketPos()

	keysPath := filepath.Join(path, sf.keysFile.Name())
	keysCfd, err := fsx.NewCacheFD(keysPath, keysMaxFdHold)
	if err != nil {
		return err
	}

	if err := db.loadKeys(keysCfd, bucketPos, sf.seqID); err != nil {
		return err
	}

	bf, err := loadBloomFilter(dataCfd, footer)
	if err != nil {
		return err
	}

	diskSeg := newDiskSegment(sf.seqID, int(dataCfd.Size()), db.path, bf, bucketPos)
	diskSeg.clearAtFn = db.getBucketClearAt
	diskSeg.keysCfd = keysCfd
	diskSeg.dataCfd = dataCfd

	db.diskSegs = append(db.diskSegs, diskSeg)
	db.stats.diskSegment.Add(1)
	db.diskSegs.OrderDecrement()

	for _, bucketMeta := range bms {
		name := bucketMeta.Name
		keySet := db.bucketsKeys.GetOrCreate(name)
		if _, ok := db.memorySegs[name]; !ok {
			memSeg := newMemorySegment(name, memSegCallback{
				onRemove: keySet.Remove,
				onInsert: keySet.Insert,
				onExist:  keySet.Has,
				onBytes:  func(n int64) { db.markBytesCh <- n },
			})
			memSeg.clearAt = db.clearAt[name]
			db.memorySegs[name] = memSeg
			db.stats.memSegment.Add(1)
		}

		bucket := &Bucket{
			name:            name,
			keys:            keySet,
			head:            db.memorySegs[name],
			statistics:      db.stats,
			getIterReleaser: db.getIterReleaser,
		}
		db.buckets[name] = bucket
	}

	return nil
}
