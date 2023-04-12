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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chenjiandongx/grogudb/pkg/codec"
)

func TestCompact(t *testing.T) {
	t.Run("Compact: 0->0", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 100 * KB
		testCompact(t, 450, opt, 0, 0)
	})

	t.Run("Compact: 1->1", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 10 * KB
		testCompact(t, 450, opt, 1, 1)
	})

	t.Run("Compact: 5->1", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 10 * KB
		testCompact(t, 2000, opt, 5, 1)
	})

	t.Run("Compact: 8->1", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 10 * KB
		testCompact(t, 3000, opt, 7, 1)
	})

	t.Run("Compact: 8->3", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 10 * KB
		opt.MaxDiskSegmentBytes = 50 * KB
		testCompact(t, 3000, opt, 7, 2)
	})
}

func testCompact(t *testing.T, iter int, opt Options, beforeSeg, afterSeg int64) {
	runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
		bucket := db.GetOrCreateBucket(bucketNum(0))
		bucketPut(iter, bucket)

		require.Equal(t, beforeSeg, db.Stats().DiskSegment)
		compacted, err := db.Compact()
		require.True(t, compacted)
		require.NoError(t, err)

		db.Gc()
		require.Equal(t, afterSeg, db.Stats().DiskSegment)

		for i := 0; i < iter; i++ {
			assertGet(t, bucket.Get, i, i)
		}
	}, nil)
}

func TestSplitDiskSegmentGroups(t *testing.T) {
	t.Run("1 Group", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxDiskSegmentBytes = 10 * KB
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			segs := make([]*diskSegment, 0)
			segs = append(segs, &diskSegment{seqID: 0, size: 9 * KB})
			ret := db.splitDiskSegmentGroups(segs)
			require.Len(t, ret, 1)
			require.Equal(t, 9*KB, ret[0][0].size)
		}, nil)
	})

	t.Run("2 Groups", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxDiskSegmentBytes = 10 * KB
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			segs := make([]*diskSegment, 0)
			segs = append(segs, &diskSegment{seqID: 0, size: 9 * KB})
			segs = append(segs, &diskSegment{seqID: 1, size: 2 * KB})
			ret := db.splitDiskSegmentGroups(segs)
			require.Len(t, ret, 2)
			require.Equal(t, 9*KB, ret[0][0].size)
			require.Equal(t, 2*KB, ret[1][0].size)
		}, nil)
	})

	t.Run("2 Groups", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxDiskSegmentBytes = 10 * KB
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			segs := make([]*diskSegment, 0)
			segs = append(segs, &diskSegment{seqID: 0, size: 3 * KB})
			segs = append(segs, &diskSegment{seqID: 1, size: 2 * KB})
			segs = append(segs, &diskSegment{seqID: 2, size: 8 * KB})
			ret := db.splitDiskSegmentGroups(segs)
			require.Len(t, ret, 2)
			require.Equal(t, 3*KB, ret[0][0].size)
			require.Equal(t, 2*KB, ret[0][1].size)
			require.Equal(t, 8*KB, ret[1][0].size)
		}, nil)
	})

	t.Run("2 Groups", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxDiskSegmentBytes = 10 * KB
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			segs := make([]*diskSegment, 0)
			segs = append(segs, &diskSegment{seqID: 0, size: 3 * KB})
			segs = append(segs, &diskSegment{seqID: 1, size: 2 * KB})
			segs = append(segs, &diskSegment{seqID: 2, size: 8 * KB})
			segs = append(segs, &diskSegment{seqID: 3, size: 1 * KB})
			ret := db.splitDiskSegmentGroups(segs)
			require.Len(t, ret, 2)
			require.Equal(t, 3*KB, ret[0][0].size)
			require.Equal(t, 2*KB, ret[0][1].size)
			require.Equal(t, 8*KB, ret[1][0].size)
			require.Equal(t, 1*KB, ret[1][1].size)
		}, nil)
	})
}

func TestMergeBucketNames(t *testing.T) {
	segs := []*diskSegment{
		{
			pos: codec.BucketPos{
				Record: map[string]codec.Positions{
					"bucket1": {},
					"bucket2": {},
					"bucket5": {},
				},
			},
		},
		{
			pos: codec.BucketPos{
				Record: map[string]codec.Positions{
					"bucket1": {},
					"bucket3": {},
					"bucket5": {},
				},
			},
		},
	}
	names := mergeBucketNames(segs...)
	require.Equal(t, []string{"bucket1", "bucket2", "bucket3", "bucket5"}, names)
}
