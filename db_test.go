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

func TestDBOpen(t *testing.T) {
	t.Run("Flock", func(t *testing.T) {
		dir := makeTmpDir()
		defer removeDir(dir)

		_, err := Open(dir, nil)
		require.NoError(t, err)

		_, err = Open(dir, nil)
		require.Error(t, err)
	})

	t.Run("Empty database path", func(t *testing.T) {
		_, err := Open("", nil)
		require.Equal(t, ErrEmptyDBPath, err)
	})

	t.Run("Double close", func(t *testing.T) {
		dir := makeTmpDir()
		defer removeDir(dir)

		db, err := Open(dir, nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		require.Equal(t, ErrClosed, db.Close())
	})
}

func TestBucketName(t *testing.T) {
	dir := makeTmpDir()
	defer removeDir(dir)

	db, err := Open(dir, nil)
	require.NoError(t, err)

	db.GetOrCreateBucket("bucket1")
	db.GetOrCreateBucket("bucket3")
	db.GetOrCreateBucket("bucket2")

	buckets := db.Buckets()
	require.Equal(t, []string{"bucket1", "bucket2", "bucket3"}, buckets)
}

func TestGetMemoryBuckets(t *testing.T) {
	dir := makeTmpDir()
	defer removeDir(dir)

	db, err := Open(dir, nil)
	require.NoError(t, err)

	db.GetOrCreateBucket("bucket1")
	db.GetOrCreateBucket("bucket3")
	db.GetOrCreateBucket("bucket2")

	buckets := db.getMemoryBuckets()
	require.Len(t, buckets, 0)
}
