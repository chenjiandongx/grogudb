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

package fsx

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.uber.org/multierr"

	"github.com/chenjiandongx/grogudb/pkg/rescue"
)

const (
	FlagAppend = os.O_APPEND | os.O_CREATE | os.O_WRONLY
)

// FileDesc 文件句柄接口定义
type FileDesc interface {
	io.ReaderAt
	io.ReadCloser
	io.Seeker
	Size() int64
}

var _ FileDesc = (*fileDesc)(nil)

type fileDesc struct {
	f    *os.File
	size int64
}

func (fd *fileDesc) Size() int64 {
	return fd.size
}

func (fd *fileDesc) Seek(offset int64, whence int) (int64, error) {
	return fd.f.Seek(offset, whence)
}

func (fd *fileDesc) Read(b []byte) (int, error) {
	return fd.f.Read(b)
}

func (fd *fileDesc) ReadAt(b []byte, off int64) (int, error) {
	return fd.f.ReadAt(b, off)
}

func (fd *fileDesc) Close() error {
	return fd.f.Close()
}

// CacheFD 可缓存 fd
type CacheFD struct {
	path   string
	size   int64
	cache  chan FileDesc
	closed chan struct{}
}

// NewCacheFD 生成并返回 *CacheFD 实例
func NewCacheFD(path string, n int) (*CacheFD, error) {
	if n <= 0 {
		return nil, errors.New("negative fd cache count")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &CacheFD{
		path:   path,
		size:   info.Size(),
		cache:  make(chan FileDesc, n),
		closed: make(chan struct{}, 1),
	}, nil
}

// Path 返回 fd path
func (fd *CacheFD) Path() string {
	return fd.path
}

// Size 返回 fd 持有文件大小
func (fd *CacheFD) Size() int64 {
	return fd.size
}

// Close 关闭并清理 fd
func (fd *CacheFD) Close() error {
	close(fd.cache)

	var errs []error
	for r := range fd.cache {
		errs = append(errs, r.Close())
	}
	return multierr.Combine(errs...)
}

// FileDesc 返回 Fd
func (fd *CacheFD) FileDesc() (FileDesc, error) {
	select {
	case r := <-fd.cache:
		return r, nil
	default:
	}

	f, err := os.Open(fd.path)
	if err != nil {
		return nil, err
	}
	return &fileDesc{f: f, size: fd.size}, nil
}

// Reuse 复用 fd
func (fd *CacheFD) Reuse(f FileDesc) {
	defer rescue.HandleCrash()

	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		_ = f.Close()
		return
	}

	select {
	case <-fd.closed:
		_ = f.Close()
		return
	default:
	}

	select {
	case fd.cache <- f:
	default:
		_ = f.Close()
	}
}

const (
	PrefixDataFile = "data"
	PrefixKeysFile = "keys"
)

// DataFilename Data 文件名称
func DataFilename(seqID int64, path ...string) string {
	var p []string
	p = append(p, path...)
	p = append(p, fmt.Sprintf("%s_%d", PrefixDataFile, seqID))
	return filepath.Join(p...)
}

// DataTmpFilename 临时 Data 文件名称
func DataTmpFilename(seqID int64, path ...string) string {
	return DataFilename(seqID, path...) + ".tmp"
}

// KeysFilename Keys 文件名称
func KeysFilename(seqID int64, path ...string) string {
	var p []string
	p = append(p, path...)
	p = append(p, fmt.Sprintf("%s_%d", PrefixKeysFile, seqID))
	return filepath.Join(p...)
}

// KeysTmpFilename 临时 Keys 文件名称
func KeysTmpFilename(seqID int64, path ...string) string {
	return KeysFilename(seqID, path...) + ".tmp"
}

// ParseFilename 解析文件名
func ParseFilename(s string) (string, int64, bool) {
	parts := strings.Split(s, "_")
	if len(parts) != 2 {
		return "", 0, false
	}

	switch parts[0] {
	case PrefixDataFile, PrefixKeysFile:
	default:
		return "", 0, false
	}

	seqID, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, false
	}

	return parts[0], int64(seqID), true
}

// WriteFile 创建文件 p 并写入数据 b
func WriteFile(p string, b []byte) error {
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(b); err != nil {
		return err
	}

	return nil
}
