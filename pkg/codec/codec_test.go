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

package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chenjiandongx/grogudb/pkg/buffer"
)

func TestTombstoneCodec(t *testing.T) {
	key1 := "tombstone1"
	key2 := "tombstone2"

	t.Run("Tombstone", func(t *testing.T) {
		b := EncodeTombstoneEntity(key1, 1000)
		name, nano, err := DecodeTombstoneEntity(b[SizeUint32:])
		require.NoError(t, err)
		require.Equal(t, name, "tombstone1")
		require.Equal(t, uint64(1000), nano)
	})

	t.Run("Invalid checksum", func(t *testing.T) {
		b := EncodeTombstoneEntity(key1, 1000)
		name, nano, err := DecodeTombstoneEntity(b)
		require.Error(t, err, ErrInvalidRecordChecksum)
		require.Zero(t, name)
		require.Zero(t, nano)
	})

	t.Run("Encode-Ranger", func(t *testing.T) {
		var buf []byte
		buf = append(buf, EncodeTombstoneEntity(key1, 1000)...)
		buf = append(buf, EncodeTombstoneEntity(key2, 2000)...)

		idx := 0
		rg := NewTombstoneRanger(bytes.NewReader(buf))
		err := rg.Range(func(bucket string, nano int64) {
			switch idx {
			case 0:
				require.Equal(t, bucket, key1)
				require.Equal(t, nano, int64(1000))
			case 1:
				require.Equal(t, bucket, key2)
				require.Equal(t, nano, int64(2000))
			}
			idx++
		})
		require.Equal(t, 2, idx)
		require.NoError(t, err)
	})
}

func TestRecordCodec(t *testing.T) {
	key := []byte("grogu_bucket_test_key")
	val := []byte("grogu_bucket_test_val")

	t.Run("DecodeRecord invalid uint32 size", func(t *testing.T) {
		flag, k, v, err := DecodeRecord(make([]byte, 3))
		require.Error(t, err, ErrInvalidUint32Size)
		require.Equal(t, FlagUnset, flag)
		require.Nil(t, k)
		require.Nil(t, v)
	})

	t.Run("Encode-Ranger", func(t *testing.T) {
		b := EncodeRecord(FlagPut, key, val)
		buf := bytes.NewBuffer(b)
		rg := NewRecordRanger(buf)
		err := rg.Range(func(flag Flag, k, v []byte, n int) (quit bool) {
			require.Equal(t, FlagPut, flag)
			require.Equal(t, key, k)
			require.Equal(t, val, v)
			return false
		})
		require.NoError(t, err)
	})

	t.Run("DecodeRecord-Put", func(t *testing.T) {
		b := EncodeRecord(FlagPut, key, val)
		flag, k, v, err := DecodeRecord(b)
		require.NoError(t, err)
		require.Equal(t, FlagPut, flag)
		require.Equal(t, key, k)
		require.Equal(t, val, v)
	})

	t.Run("DecodeRecord-Del", func(t *testing.T) {
		b := EncodeRecord(FlagDel, key, nil)
		flag, k, v, err := DecodeRecord(b)
		require.NoError(t, err)
		require.Equal(t, FlagDel, flag)
		require.Equal(t, key, k)
		require.Nil(t, v)
	})

	t.Run("EncodeRecord-Tombstone", func(t *testing.T) {
		b := EncodeRecord(FlagTombstone, nil, nil)
		flag, k, v, err := DecodeRecord(b)
		require.NoError(t, err)
		require.Equal(t, FlagTombstone, flag)
		require.Nil(t, k)
		require.Nil(t, v)
	})
}

func TestKeyEntityCodec(t *testing.T) {
	t.Run("Encode invalid checksum", func(t *testing.T) {
		buf, err := VerifyTailChecksum(make([]byte, 3))
		require.Equal(t, ErrInvalidChecksum, err)
		require.Zero(t, buf)
	})

	t.Run("Encode-KeyBlock", func(t *testing.T) {
		buf := buffer.Get()
		defer buffer.Put(buf)

		_, _ = buf.Write(EncodeKeyEntity(FlagPut, 1, 100))
		_, _ = buf.Write(EncodeKeyEntity(FlagPut, 2, 200))
		_, err := VerifyTailChecksum(buf.Frozen())
		require.NoError(t, err)
	})

	t.Run("Encode-Ranger not skip del", func(t *testing.T) {
		buf := buffer.Get()
		defer buffer.Put(buf)

		_, _ = buf.Write(EncodeKeyEntity(FlagPut, 1, 100))
		_, _ = buf.Write(EncodeKeyEntity(FlagDel, 2, 200))
		b, err := VerifyTailChecksum(buf.Frozen())
		require.NoError(t, err)

		idx := 0
		rg := NewKeysRanger(bytes.NewReader(b))
		err = rg.Range(func(flag Flag, h uint64, n uint32) {
			switch idx {
			case 0:
				require.Equal(t, h, uint64(1))
				require.Equal(t, n, uint32(100))
			case 1:
				require.Equal(t, h, uint64(2))
				require.Equal(t, n, uint32(200))
			}
			idx++
		})
		require.Equal(t, 2, idx)
		require.NoError(t, err)
	})
}

func TestFooterCodec(t *testing.T) {
	t.Run("Encode Footer", func(t *testing.T) {
		footer := Footer{
			DataSize:         1,
			MetaSize:         2,
			BloomFilterCount: 3,
			BloomFilterSize:  4,
		}
		b := EncodeFooter(footer)
		require.Equal(t, 20, len(b))

		f, err := DecodeFooter(b)
		require.NoError(t, err)
		require.Equal(t, footer, f)
	})

	t.Run("Footer invalid size", func(t *testing.T) {
		b := make([]byte, 19)
		_, err := DecodeFooter(b)
		require.Equal(t, ErrInvalidFooterSize, err)
	})

	t.Run("Footer invalid magic", func(t *testing.T) {
		b := make([]byte, 20)
		_, err := DecodeFooter(b)
		require.Equal(t, ErrInvalidFooterMagic, err)
	})
}

func TestMetadataCodec(t *testing.T) {
	t.Run("Encode Metadata", func(t *testing.T) {
		var bms BucketMetaSlice
		bms = append(bms, BucketMeta{
			Name: "bucket1",
			RecordPos: []Position{
				{Start: 10, End: 20},
				{Start: 20, End: 30},
				{Start: 30, End: 40},
			},
			KeyEntityPos: []Position{
				{Start: 10, End: 20},
				{Start: 20, End: 30},
				{Start: 30, End: 40},
			},
		})
		bms = append(bms, BucketMeta{
			Name: "bucket2",
			RecordPos: []Position{
				{Start: 40, End: 50},
				{Start: 50, End: 60},
				{Start: 60, End: 70},
			},
			KeyEntityPos: []Position{
				{Start: 40, End: 50},
				{Start: 50, End: 60},
				{Start: 60, End: 70},
			},
		})

		b := EncodeMetadata(bms)
		data, err := DecodeMetadata(b)
		require.NoError(t, err)
		require.Equal(t, bms, data)
	})

	t.Run("Metadata invalid checksum", func(t *testing.T) {
		b := make([]byte, 8)
		_, err := DecodeMetadata(b)
		require.Equal(t, ErrInvalidMetadataChecksum, err)
	})
}
