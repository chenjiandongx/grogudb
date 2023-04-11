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
)

func TestCompact(t *testing.T) {
	t.Run("Compact: 0->0", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 1024 * 100
		testCompact(t, 450, opt, 0, 0)
	})

	t.Run("Compact: 1->1", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 1024 * 10
		testCompact(t, 450, opt, 1, 1)
	})

	t.Run("Compact: 5->1", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 1024 * 10
		testCompact(t, 2000, opt, 5, 1)
	})

	t.Run("Compact: 8->1", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 1024 * 10
		testCompact(t, 3000, opt, 7, 1)
	})

	t.Run("Compact: 8->3", func(t *testing.T) {
		opt := DefaultOptions()
		opt.MaxMemSegmentBytes = 1024 * 10
		opt.MaxDiskSegmentBytes = 1024 * 50
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
