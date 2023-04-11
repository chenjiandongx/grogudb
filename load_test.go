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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chenjiandongx/grogudb/pkg/codec"
	"github.com/chenjiandongx/grogudb/pkg/fsx"
)

type mockEntity struct {
	name string
	dir  bool
}

func (e mockEntity) Name() string {
	return e.name
}

func (e mockEntity) IsDir() bool {
	return e.dir
}

func (e mockEntity) Type() os.FileMode {
	return os.ModePerm
}

func (e mockEntity) Info() (os.FileInfo, error) {
	return nil, nil
}

func TestSplitEntries(t *testing.T) {
	t.Run("Got Two", func(t *testing.T) {
		entities := []os.DirEntry{
			mockEntity{name: "data_0"},
			mockEntity{name: "data_1"},
			mockEntity{name: "keys_0"},
			mockEntity{name: "keys_1"},
		}

		sfs := splitEntries(entities)
		require.Len(t, sfs, 2)
		require.Equal(t, int64(0), sfs[0].seqID)
		require.Equal(t, int64(1), sfs[1].seqID)
	})

	t.Run("Got One", func(t *testing.T) {
		entities := []os.DirEntry{
			mockEntity{name: "data_0"},
			mockEntity{name: "data_1"},
			mockEntity{name: "keys_0"},
			mockEntity{name: "keys_2"},
		}

		sfs := splitEntries(entities)
		require.Len(t, sfs, 1)
		require.Equal(t, int64(0), sfs[0].seqID)
	})

	t.Run("Got Zero", func(t *testing.T) {
		entities := []os.DirEntry{
			mockEntity{name: "data_0"},
			mockEntity{name: "data_1"},
			mockEntity{name: "keys_0", dir: true},
			mockEntity{name: "keys_2"},
		}

		sfs := splitEntries(entities)
		require.Len(t, sfs, 0)
	})
}

func getDataFd(t require.TestingT, db *DB) *fsx.CacheFD {
	bucket := db.GetOrCreateBucket("bucket0")
	require.NoError(t, bucket.Put(keyNum(0), valNum(0)))
	require.NoError(t, db.rotate())

	entities, err := os.ReadDir(db.path)
	require.NoError(t, err)

	var dataF os.DirEntry
	for _, entity := range entities {
		if strings.HasPrefix(entity.Name(), fsx.PrefixDataFile) {
			dataF = entity
		}
	}

	cfd, err := fsx.NewCacheFD(filepath.Join(db.path, dataF.Name()), 1)
	require.NoError(t, err)
	return cfd
}

func TestLoadFooter(t *testing.T) {
	runGrogudbTest(t, nil, func(t require.TestingT, db *DB) {
		cfd := getDataFd(t, db)
		footer, err := loadFooter(cfd)
		require.NoError(t, err)
		require.Equal(t, codec.Footer{
			DataSize:         25,
			MetaSize:         35,
			BloomFilterCount: 1,
			BloomFilterSize:  32,
		}, footer)
	}, nil)
}

func TestLoadMetadata(t *testing.T) {
	runGrogudbTest(t, nil, func(t require.TestingT, db *DB) {
		cfd := getDataFd(t, db)
		footer, err := loadFooter(cfd)
		require.NoError(t, err)

		meta, err := loadMetadata(cfd, footer)
		require.NoError(t, err)
		require.Equal(t, codec.BucketMetaSlice{
			{
				Name:         "bucket0",
				RecordPos:    codec.Positions{{Start: 0, End: 25}},
				KeyEntityPos: codec.Positions{{Start: 0, End: 17}},
			},
		}, meta)
	}, nil)
}

func TestLoadBloomFilter(t *testing.T) {
	runGrogudbTest(t, nil, func(t require.TestingT, db *DB) {
		cfd := getDataFd(t, db)
		footer, err := loadFooter(cfd)
		require.NoError(t, err)

		bf, err := loadBloomFilter(cfd, footer)
		require.NoError(t, err)

		require.True(t, bf.Test(codec.HashKey(keyNum(0))))
		require.False(t, bf.Test(codec.HashKey(keyNum(1))))
	}, nil)
}
