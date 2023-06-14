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
	"errors"
	"hash/crc32"
	"io"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/chenjiandongx/grogudb/pkg/binaryx"
)

var (
	ErrInvalidFlagSize          = errors.New("grogudb/codec: invalid flag size")
	ErrInvalidUint32Size        = errors.New("grogudb/codec: invalid uint32 size")
	ErrInvalidUint64Size        = errors.New("grogudb/codec: invalid uint64 size")
	ErrInvalidRecordSize        = errors.New("grogudb/codec: invalid record size")
	ErrInvalidRecordChecksum    = errors.New("grogudb/codec: invalid record checksum")
	ErrInvalidRecordKeySize     = errors.New("grogudb/codec: invalid record key size")
	ErrInvalidRecordValueSize   = errors.New("grogudb/codec: invalid record value size")
	ErrInvalidMetadataChecksum  = errors.New("grogudb/codec: invalid metadata checksum")
	ErrInvalidTombstoneChecksum = errors.New("grogudb/codec: invalid tombstone checksum")
	ErrInvalidFooterSize        = errors.New("grogudb/codec: invalid footer size")
	ErrInvalidFooterMagic       = errors.New("grogudb/codec: invalid footer magic")
	ErrReadPartial              = errors.New("grogudb/codec: read partial")
	ErrInvalidChecksum          = errors.New("grogudb/codec: invalid checksum")
)

const (
	magicNumber uint32 = 0xdeadbeef
)

const (
	SizeFlag     = 1
	SizeUint32   = 4
	SizeUint64   = 8
	SizePosition = 8
	SizeChecksum = 4
)

// CRC32 计算 b crc32 checksum
func CRC32(b []byte) []byte {
	return binaryx.PutUint32(crc32.ChecksumIEEE(b))
}

type Flag uint8

const (
	FlagUnset Flag = iota
	FlagPut
	FlagDel
	FlagTombstone
)

type (
	RecordVisitFunc func(flag Flag, key, val []byte, n int) (quit bool)
	KeyVisitFunc    func(flag Flag, h uint64, n uint32)
	PassFunc        func(flag Flag, key uint64) bool
)

// PassAll PassFunc 放行所有条件
func PassAll() PassFunc {
	return func(Flag, uint64) bool {
		return true
	}
}

// HashKey hash key 使用 uint64 表示
func HashKey(b []byte) uint64 {
	return xxhash.Sum64(b)
}

const (
	FooterSize = 20
)

// Footer 尾部内容描述字节区间
type Footer struct {
	DataSize         uint32
	MetaSize         uint32
	BloomFilterCount uint32
	BloomFilterSize  uint32
}

// PosDataBlock 返回 DataBlock Position
func (f Footer) PosDataBlock() Position {
	return Position{
		Start: 0,
		End:   f.DataSize,
	}
}

// PosMetaBlock 返回 MetaBlock Position
func (f Footer) PosMetaBlock() Position {
	return Position{
		Start: f.DataSize,
		End:   f.DataSize + f.MetaSize,
	}
}

// PosBloomFilterBlock 返回 BloomFilter Position
func (f Footer) PosBloomFilterBlock() Position {
	return Position{
		Start: f.DataSize + f.MetaSize,
		End:   f.BloomFilterSize,
	}
}

// EncodeFooter 编码 Footer
//
// 布局结构如下
// | DataSize | MetaSize | BloomFilterCount | BloomFilterBytes | Magic |
// | 4B       | 4B       | 4B               | 4B               | 4B    |
func EncodeFooter(footer Footer) []byte {
	buf := make([]byte, 0, FooterSize)
	buf = append(buf, binaryx.PutUint32(footer.DataSize)...)
	buf = append(buf, binaryx.PutUint32(footer.MetaSize)...)
	buf = append(buf, binaryx.PutUint32(footer.BloomFilterCount)...)
	buf = append(buf, binaryx.PutUint32(footer.BloomFilterSize)...)
	buf = append(buf, binaryx.PutUint32(magicNumber)...)
	return buf
}

// DecodeFooter 解码 Footer
func DecodeFooter(b []byte) (Footer, error) {
	if len(b) != FooterSize {
		return Footer{}, ErrInvalidFooterSize
	}

	if binaryx.Uint32(b[16:]) != magicNumber {
		return Footer{}, ErrInvalidFooterMagic
	}

	footer := Footer{
		DataSize:         binaryx.Uint32(b[:4]),
		MetaSize:         binaryx.Uint32(b[4:8]),
		BloomFilterCount: binaryx.Uint32(b[8:12]),
		BloomFilterSize:  binaryx.Uint32(b[12:16]),
	}

	return footer, nil
}

// EncodeTombstoneEntity 编码 TombstoneEntity
//
// 布局结构如下
// | RecordSize | BucketSize | Bucket | Nanosecond | Checksum |
// | 4B         | 4B         | ...    | 8B         | 4B       |
func EncodeTombstoneEntity(name string, nano uint64) []byte {
	size := len(name) + 16

	buf := make([]byte, 0, size+SizeUint32)
	buf = append(buf, binaryx.PutUint32(uint32(size))...)
	buf = append(buf, binaryx.PutUint32(uint32(len(name)))...)
	buf = append(buf, []byte(name)...)
	buf = append(buf, binaryx.PutUint64(nano)...)
	buf = append(buf, CRC32(buf[SizeUint32:])...)
	return buf
}

// DecodeTombstoneEntity 解码 TombstoneEntity
func DecodeTombstoneEntity(b []byte) (string, uint64, error) {
	if len(b) <= SizeChecksum {
		return "", 0, ErrInvalidTombstoneChecksum
	}

	if !bytes.Equal(CRC32(b[:len(b)-SizeChecksum]), b[len(b)-SizeChecksum:]) {
		return "", 0, ErrInvalidTombstoneChecksum
	}

	buffer := bytes.NewBuffer(b)

	// 解析 Bucket
	sizeBytes := buffer.Next(SizeUint32)
	if len(sizeBytes) != SizeUint32 {
		return "", 0, ErrInvalidUint32Size
	}
	bucketSize := binaryx.Uint32(sizeBytes)
	bucket := buffer.Next(int(bucketSize))
	if len(bucket) != int(bucketSize) {
		return "", 0, ErrInvalidRecordKeySize
	}

	// 解析 Nanosecond
	sizeBytes = buffer.Next(SizeUint64)
	if len(sizeBytes) != SizeUint64 {
		return "", 0, ErrInvalidUint32Size
	}
	nano := binaryx.Uint64(sizeBytes)

	return string(bucket), nano, nil
}

// TombstoneRanger 负责遍历解析 io.Reader Tombstone 数据流
type TombstoneRanger struct {
	r io.Reader
}

// NewTombstoneRanger 生成并返回 *TombstoneRanger 实例
func NewTombstoneRanger(r io.Reader) *TombstoneRanger {
	return &TombstoneRanger{r: r}
}

// Range 遍历所有 TombstoneEntity 并对每个 Entity 执行 fn
func (rg *TombstoneRanger) Range(fn func(bucket string, nano int64)) error {
	sizeBytes := make([]byte, SizeUint32)
	for {
		n, err := rg.r.Read(sizeBytes)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n != SizeUint32 {
			return err
		}

		size := binaryx.Uint32(sizeBytes)
		b := make([]byte, size)
		n, err = rg.r.Read(b)
		if err != nil {
			return err
		}
		if n != int(size) {
			return ErrReadPartial
		}

		bucket, nano, err := DecodeTombstoneEntity(b)
		if err != nil {
			return err
		}

		fn(bucket, int64(nano))
	}
}

// EncodeRecord 编码 Record
//
// Record 布局结构如下
// | RecordSize | Flag | KeySize | Key | ValueSize | Value |
// | 4B         | 1B   | 4B      | ... | 4B        | ...   |
func EncodeRecord(flag Flag, key, val []byte) []byte {
	size := len(key) + len(val) + 9

	buf := make([]byte, 0, size)
	buf = append(buf, binaryx.PutUint32(uint32(size))...)
	buf = append(buf, byte(flag))
	buf = append(buf, binaryx.PutUint32(uint32(len(key)))...)
	buf = append(buf, key...)
	buf = append(buf, binaryx.PutUint32(uint32(len(val)))...)
	buf = append(buf, val...)

	return buf
}

// DecodeRecord memory segment 的 record 是包含了 size header 解析时候需要 offset
func DecodeRecord(b []byte) (Flag, []byte, []byte, error) {
	if len(b) < SizeUint32 {
		return FlagUnset, nil, nil, ErrInvalidRecordSize
	}
	return DecodeRecordWithoutSize(b[SizeUint32:])
}

// DecodeRecordWithoutSize 解码 Record
func DecodeRecordWithoutSize(b []byte) (Flag, []byte, []byte, error) {
	buffer := bytes.NewBuffer(b)
	flag, err := buffer.ReadByte()
	if err != nil {
		return FlagUnset, nil, nil, err
	}

	// FlagTombstone 跳过不做解析
	if Flag(flag) == FlagTombstone {
		return Flag(flag), nil, nil, nil
	}

	// 解析 Key
	sizeBytes := buffer.Next(SizeUint32)
	if len(sizeBytes) != SizeUint32 {
		return FlagUnset, nil, nil, ErrInvalidUint32Size
	}
	keySize := binaryx.Uint32(sizeBytes)
	key := buffer.Next(int(keySize))
	if len(key) != int(keySize) {
		return FlagUnset, nil, nil, ErrInvalidRecordKeySize
	}

	// 解析 Value
	sizeBytes = buffer.Next(SizeUint32)
	if len(sizeBytes) != SizeUint32 {
		return FlagUnset, nil, nil, ErrInvalidUint32Size
	}

	var val []byte
	valSize := binaryx.Uint32(sizeBytes)
	if valSize > 0 {
		val = buffer.Next(int(valSize))
		if len(val) != int(valSize) {
			return FlagUnset, nil, nil, ErrInvalidRecordValueSize
		}
	}

	return Flag(flag), key, val, nil
}

// RecordRanger 负责遍历解析 io.Reader Record 数据流
type RecordRanger struct {
	r io.Reader
}

// NewRecordRanger 生产并返回 *RecordRanger 实例
func NewRecordRanger(r io.Reader) *RecordRanger {
	return &RecordRanger{r: r}
}

func (rg *RecordRanger) next() ([]byte, error) {
	sizeBytes := make([]byte, SizeUint32)
	n, err := rg.r.Read(sizeBytes)
	if err != nil {
		return nil, err
	}
	if n != SizeUint32 {
		return nil, ErrInvalidUint32Size
	}

	dataSize := binaryx.Uint32(sizeBytes)
	data := make([]byte, dataSize)
	n, err = rg.r.Read(data)
	if err != nil {
		return nil, err
	}
	if n != int(dataSize) {
		return nil, ErrInvalidRecordSize
	}

	return data, nil
}

// Range 遍历所有 Records 并对每个 Record 执行 visitFn
func (rg *RecordRanger) Range(visitFn RecordVisitFunc) error {
	for {
		b, err := rg.next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		flag, key, val, err := DecodeRecordWithoutSize(b)
		if err != nil {
			return err
		}

		if flag == FlagTombstone {
			return nil
		}

		quit := visitFn(flag, key, val, len(b))
		if quit {
			return nil
		}
	}
}

// EncodeKeyEntity 编码 KeyEntity
//
// 布局如下
// | Flag | KeyHash | RecordSize |
// | 1B   | 8B      | 4B         |
func EncodeKeyEntity(flag Flag, h uint64, l uint32) []byte {
	b := make([]byte, 0, 1+SizeUint64+SizeUint32)
	b = append(b, byte(flag))
	b = append(b, binaryx.PutUint64(h)...)
	b = append(b, binaryx.PutUint32(l)...)
	return b
}

// VerifyTailChecksum 校验尾部 checksum 并返回被校验数据
func VerifyTailChecksum(b []byte) ([]byte, error) {
	if len(b) < SizeChecksum {
		return nil, ErrInvalidChecksum
	}

	if !bytes.Equal(CRC32(b[:len(b)-SizeChecksum]), b[len(b)-SizeChecksum:]) {
		return nil, ErrInvalidChecksum
	}

	return b[:len(b)-SizeChecksum], nil
}

// KeysRanger 负责遍历解析 io.Reader Keys 数据流
type KeysRanger struct {
	r io.Reader
}

// NewKeysRanger 生成并返回 *KeysRanger 实例
func NewKeysRanger(r io.Reader) *KeysRanger {
	return &KeysRanger{r: r}
}

// Range 遍历所有 HashKeys 并对每个 Key 执行 fn
func (rg *KeysRanger) Range(fn func(flag Flag, h uint64, n uint32)) error {
	flagBytes := make([]byte, SizeFlag)
	hashBytes := make([]byte, SizeUint64)
	lenBytes := make([]byte, SizeUint32)

	for {
		n, err := rg.r.Read(flagBytes)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n != SizeFlag {
			return ErrInvalidFlagSize
		}
		flag := Flag(flagBytes[0])

		n, err = rg.r.Read(hashBytes)
		if err != nil {
			return err
		}
		if n != SizeUint64 {
			return ErrInvalidUint64Size
		}
		h := binaryx.Uint64(hashBytes)

		n, err = rg.r.Read(lenBytes)
		if err != nil {
			return err
		}
		if n != SizeUint32 {
			return ErrInvalidUint32Size
		}
		l := binaryx.Uint32(lenBytes)
		fn(flag, h, l)
	}
}

// Position 表示索引字节位置
type Position struct {
	Start uint32
	End   uint32
}

// BucketMeta Bucket 元数据信息 包含 bucket 名称以及索引字节位置
type BucketMeta struct {
	Name         string
	RecordPos    []Position
	KeyEntityPos []Position
}

// BucketMetaSlice BucketMeta 列表
type BucketMetaSlice []BucketMeta

// AsBucketPos 将 BucketMetaSlice 转换成 BucketPos
func (bms BucketMetaSlice) AsBucketPos() BucketPos {
	recordPos := make(map[string]Positions)
	for _, bm := range bms {
		recordPos[bm.Name] = bm.RecordPos
	}
	keyEntityPos := make(map[string]Positions)
	for _, bm := range bms {
		keyEntityPos[bm.Name] = bm.KeyEntityPos
	}

	return BucketPos{
		Record:  recordPos,
		KeyItem: keyEntityPos,
	}
}

type Positions []Position

// MaxRange 取 Positions 最大区间
func (ps Positions) MaxRange() uint32 {
	var max uint32
	for _, pos := range ps {
		delta := pos.End - pos.Start
		if delta > max {
			max = delta
		}
	}
	return max
}

// BucketPos Bucket 索引字节位置信息
type BucketPos struct {
	Record  map[string]Positions // Record Position 信息
	KeyItem map[string]Positions // KeyItem Position 信息
}

// AsBucketMetaSlice 将 AsBucketMetaSlice 转换成 BucketMetaSlice
func (bp BucketPos) AsBucketMetaSlice() BucketMetaSlice {
	bms := make(BucketMetaSlice, 0, len(bp.Record))
	names := make([]string, 0, len(bp.Record))
	for name := range bp.Record {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		bms = append(bms, BucketMeta{
			Name:         name,
			RecordPos:    bp.Record[name],
			KeyEntityPos: bp.KeyItem[name],
		})
	}
	return bms
}

// EncodeMetadata 编码 Metadata
//
// 布局如下
// | BucketSize | Bucket | RecordPosCount | RecordPosBlock | KeyEntityPosBlock | Checksum |
// | 4B         | ..     | 4B             | ...            | ...               | 4B       |
//
// RecordPosBlock/KeyEntityPosBlock 布局
// | PositionStart | PositionEnd |
// | 4B            | 4B          |
func EncodeMetadata(bms BucketMetaSlice) []byte {
	var size int
	for _, bucket := range bms {
		size += SizeUint32 * 2
		size += len(bucket.Name)
		size += len(bucket.RecordPos) * SizePosition
		size += len(bucket.KeyEntityPos) * SizePosition
	}

	buf := make([]byte, 0, size+SizeUint32)
	for _, bucket := range bms {
		// Bucket Name
		buf = append(buf, binaryx.PutUint32(uint32(len(bucket.Name)))...)
		buf = append(buf, []byte(bucket.Name)...)

		// Positions
		buf = append(buf, binaryx.PutUint32(uint32(len(bucket.RecordPos)))...)
		for _, pos := range bucket.RecordPos {
			buf = append(buf, binaryx.PutUint32(pos.Start)...)
			buf = append(buf, binaryx.PutUint32(pos.End)...)
		}
		for _, pos := range bucket.KeyEntityPos {
			buf = append(buf, binaryx.PutUint32(pos.Start)...)
			buf = append(buf, binaryx.PutUint32(pos.End)...)
		}
	}

	buf = append(buf, CRC32(buf[:])...)
	return buf
}

// DecodeMetadata 解码 Metadata
func DecodeMetadata(b []byte) (BucketMetaSlice, error) {
	if len(b) < SizeChecksum {
		return nil, ErrInvalidMetadataChecksum
	}

	if !bytes.Equal(CRC32(b[:len(b)-SizeChecksum]), b[len(b)-SizeChecksum:]) {
		return nil, ErrInvalidMetadataChecksum
	}

	b = b[:len(b)-SizeChecksum]

	var bms BucketMetaSlice
	buffer := bytes.NewBuffer(b)
	sizeBytes := make([]byte, SizeUint32)
	for {
		// read bucket size
		n, err := buffer.Read(sizeBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n != SizeUint32 {
			return nil, ErrInvalidUint32Size
		}

		// read bucket name
		bucketSize := binaryx.Uint32(sizeBytes)
		bucketName := make([]byte, bucketSize)
		n, err = buffer.Read(bucketName)
		if err != nil {
			return nil, err
		}
		if n != int(bucketSize) {
			return nil, ErrReadPartial
		}

		// read position count
		n, err = buffer.Read(sizeBytes)
		if err != nil {
			return nil, err
		}
		if n != SizeUint32 {
			return nil, ErrInvalidUint32Size
		}

		// read positions
		posCnt := int(binaryx.Uint32(sizeBytes))
		recordPos := make([]Position, 0, posCnt)
		for i := 0; i < posCnt; i++ {
			n, err = buffer.Read(sizeBytes)
			if err != nil {
				return nil, err
			}
			if n != SizeUint32 {
				return nil, ErrInvalidUint32Size
			}
			start := binaryx.Uint32(sizeBytes)

			n, err = buffer.Read(sizeBytes)
			if err != nil {
				return nil, err
			}
			if n != SizeUint32 {
				return nil, ErrInvalidUint32Size
			}
			end := binaryx.Uint32(sizeBytes)

			recordPos = append(recordPos, Position{
				Start: start,
				End:   end,
			})
		}

		keyEntityPos := make([]Position, 0, posCnt)
		for i := 0; i < posCnt; i++ {
			n, err = buffer.Read(sizeBytes)
			if err != nil {
				return nil, err
			}
			if n != SizeUint32 {
				return nil, ErrInvalidUint32Size
			}
			start := binaryx.Uint32(sizeBytes)

			n, err = buffer.Read(sizeBytes)
			if err != nil {
				return nil, err
			}
			if n != SizeUint32 {
				return nil, ErrInvalidUint32Size
			}
			end := binaryx.Uint32(sizeBytes)

			keyEntityPos = append(keyEntityPos, Position{
				Start: start,
				End:   end,
			})
		}

		bms = append(bms, BucketMeta{
			Name:         string(bucketName),
			RecordPos:    recordPos,
			KeyEntityPos: keyEntityPos,
		})
	}

	return bms, nil
}
