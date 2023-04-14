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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenjiandongx/logger"
	"github.com/stretchr/testify/require"

	"github.com/chenjiandongx/grogudb/pkg/logx"
)

const (
	KB = 1024
	MB = 1024 * KB
)

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func makeTmpDir() string {
	dir, err := os.MkdirTemp("", "grogudb-test")
	// fmt.Println("mkdir:", dir)
	if err != nil {
		panic(err)
	}
	return dir
}

func keyNum(i int) []byte {
	return []byte(fmt.Sprintf("key%d", i))
}

func valNum(i int) []byte {
	return []byte(fmt.Sprintf("val%d", i))
}

func bucketNum(i int) string {
	return fmt.Sprintf("bucket%d", i)
}

func runGrogudbTest(t require.TestingT, opts *Options, open, reOpen func(t require.TestingT, db *DB)) {
	l := logger.New(logger.Options{
		Stdout:      true,
		ConsoleMode: true,
		Level:       logger.ErrorLevel,
	})
	logx.SetLogger(l)

	dir := makeTmpDir()
	defer removeDir(dir)

	db, err := Open(dir, opts)
	require.NoError(t, err)
	open(t, db)
	require.NoError(t, db.Close())

	if reOpen != nil {
		db, err = Open(dir, opts)
		require.NoError(t, err)
		reOpen(t, db)
		require.NoError(t, db.Close())
	}
}

func runParallel(n int, f func(int)) {
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		j := i
		go func() {
			defer wg.Done()
			f(j)
		}()
	}
	wg.Wait()
}

func bucketPut(n int, bucket *Bucket) {
	for i := 0; i < n; i++ {
		_ = bucket.Put(keyNum(i), valNum(i))
	}
}

func bucketPutIf(n int, bucket *Bucket) {
	for i := 0; i < n; i++ {
		_ = bucket.PutIf(keyNum(i), valNum(i))
	}
}

func assertGet(t require.TestingT, fn func(b []byte) (Bytes, error), keyN, valN int) {
	val, err := fn(keyNum(keyN))
	require.NoError(t, err)
	require.Equal(t, string(valNum(valN)), val.String())
}

func assertRange(t require.TestingT, bucket *Bucket, iter int) {
	j := iter - 1
	err := bucket.Range(func(key, val Bytes) {
		require.Equal(t, valNum(j), val.B())
		j--
	})
	require.Equal(t, -1, j)
	require.NoError(t, err)
}

func assertFastRange(t require.TestingT, bucket *Bucket, iter int) {
	j := iter - 1
	err := bucket.FastRange(func(key, val Bytes) {
		require.Equal(t, valNum(j), val.B())
		j--
	})
	require.Equal(t, -1, j)
	require.NoError(t, err)
}

const (
	bucketCount   = 2
	iterCount     = 500
	maxMemorySize = 10 * KB
)

func TestEmpty(t *testing.T) {
	dir := makeTmpDir()
	defer removeDir(dir)

	db, err := Open(dir, nil)
	require.NoError(t, err)

	bucket := db.GetOrCreateBucket("bucket")

	_, err = bucket.Get(nil)
	require.Equal(t, ErrEmtpyRecordKey, err)
	require.Equal(t, ErrEmtpyRecordKey, bucket.Put(nil, []byte("1")))
	require.Equal(t, ErrEmtpyRecordValue, bucket.Put([]byte("1"), nil))
	require.Equal(t, ErrEmtpyRecordKey, bucket.PutIf(nil, []byte("1")))
	require.Equal(t, ErrEmtpyRecordValue, bucket.PutIf([]byte("1"), nil))
	require.Equal(t, ErrEmtpyRecordKey, bucket.Del(nil))
}

func TestBucketPutGet(t *testing.T) {
	// Key 不重名 Put 正常写入
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						assertGet(t, bucket.Get, j, j)
					}
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						assertGet(t, bucket.Get, j, j)
					}
				})
			},
		)
	})

	t.Run("Compat-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)
				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						assertGet(t, bucket.Get, j, j)
					}
				})
			},
		)
	})
}

func TestBucketPutIfGet(t *testing.T) {
	// Key 不重名 Put/PutIf 行为保持一致
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPutIf(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						assertGet(t, bucket.Get, j, j)
					}
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPutIf(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						assertGet(t, bucket.Get, j, j)
					}
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPutIf(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)
				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						assertGet(t, bucket.Get, j, j)
					}
				})
			},
		)
	})
}

func TestBucketPutOverwrite(t *testing.T) {
	// Key 重名 Put 更新 Val
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.Put(keyNum(0), valNum(j))
					}
				})
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					assertGet(t, bucket.Get, 0, iterCount-1)
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.Put(keyNum(0), valNum(j))
					}
				})
				require.Equal(t, int64(0), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					assertGet(t, bucket.Get, 0, iterCount-1)
				})
			},
		)

		t.Run("Compact-ReOpen", func(t *testing.T) {
			runGrogudbTest(t, &opt,
				func(t require.TestingT, db *DB) {
					runParallel(bucketCount, func(i int) {
						bucket := db.GetOrCreateBucket(bucketNum(i))
						for j := 0; j < iterCount; j++ {
							_ = bucket.Put(keyNum(0), valNum(j))
						}
					})
					require.Equal(t, int64(0), db.Stats().DiskSegment)
					cpm, err := db.Compact()
					require.True(t, cpm)
					require.NoError(t, err)
					db.Gc()
					require.Equal(t, int64(0), db.Stats().DiskSegment)
				},
				func(t require.TestingT, db *DB) {
					runParallel(bucketCount, func(i int) {
						bucket := db.GetOrCreateBucket(bucketNum(i))
						assertGet(t, bucket.Get, 0, iterCount-1)
					})
				},
			)
		})
	})
}

func TestBucketPutIfOverwrite(t *testing.T) {
	// Key 重名 PutIf 保持第一个值
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					assertGet(t, bucket.Get, 0, 0)
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					assertGet(t, bucket.Get, 0, 0)
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
				require.Equal(t, int64(0), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)
				db.Gc()
				require.Equal(t, int64(0), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					assertGet(t, bucket.Get, 0, 0)
				})
			},
		)
	})
}

func TestBucketHas(t *testing.T) {
	// Key 不重名 Put 正常写入
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						require.True(t, bucket.Has(keyNum(j)))
					}
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						require.True(t, bucket.Has(keyNum(j)))
					}
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)
				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						require.True(t, bucket.Has(keyNum(j)))
					}
				})
			},
		)
	})
}

func TestBucketPutCount(t *testing.T) {
	// Key 不重名 Put 正常写入
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				runParallel(bucketCount, func(i int) {
					require.Equal(t, iterCount, db.GetOrCreateBucket(bucketNum(i)).Count())
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					require.Equal(t, iterCount, db.GetOrCreateBucket(bucketNum(i)).Count())
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)
				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					require.Equal(t, iterCount, db.GetOrCreateBucket(bucketNum(i)).Count())
				})
			},
		)
	})
}

func TestBucketPutIfCount(t *testing.T) {
	// Key 重名 PutIf 保持第一个值
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
				runParallel(bucketCount, func(i int) {
					require.Equal(t, 1, db.GetOrCreateBucket(bucketNum(i)).Count())
				})
			}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					require.Equal(t, 1, db.GetOrCreateBucket(bucketNum(i)).Count())
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
				require.Equal(t, int64(0), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)
				db.Gc()
				require.Equal(t, int64(0), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					require.Equal(t, 1, db.GetOrCreateBucket(bucketNum(i)).Count())
				})
			},
		)
	})
}

func TestBucketPutRange(t *testing.T) {
	// Key 不重名 Put 正常写入
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			runParallel(bucketCount, func(i int) {
				bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
			})
			runParallel(bucketCount, func(i int) {
				assertRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					assertRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)

				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					assertRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
				})
			},
		)
	})

	t.Run("Compact-ReOpen FastRange", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)

				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					assertFastRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
				})
			},
		)
	})
}

func TestBucketPutIfRange(t *testing.T) {
	// Key 不重名 Put/PutIf 行为保持一致
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			runParallel(bucketCount, func(i int) {
				bucketPutIf(iterCount, db.GetOrCreateBucket(bucketNum(i)))
			})
			runParallel(bucketCount, func(i int) {
				assertRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPutIf(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					assertRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucketPutIf(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)

				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					assertRange(t, db.GetOrCreateBucket(bucketNum(i)), iterCount)
				})
			},
		)
	})
}

func TestBucketPutRangeOverwrite(t *testing.T) {
	// Key 重名 Put 更新 Val
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				for j := 0; j < iterCount; j++ {
					_ = bucket.Put(keyNum(0), valNum(j))
				}
			})
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				j := 0
				err := bucket.Range(func(key, val Bytes) {
					j++
					require.Equal(t, valNum(iterCount-1), val.B())
				})
				require.Equal(t, 1, j)
				require.NoError(t, err)
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.Put(keyNum(0), valNum(j))
					}
				})
				require.Equal(t, int64(0), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					j := 0
					err := bucket.Range(func(key, val Bytes) {
						j++
						require.Equal(t, valNum(iterCount-1), val.B())
					})
					require.Equal(t, 1, j)
					require.NoError(t, err)
				})
			},
		)
	})

	t.Run("Compact-ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.Put(keyNum(0), valNum(j))
					}
				})
				require.Equal(t, int64(0), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)

				db.Gc()
				require.Equal(t, int64(0), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					j := 0
					err := bucket.Range(func(key, val Bytes) {
						j++
						require.Equal(t, valNum(iterCount-1), val.B())
					})
					require.Equal(t, 1, j)
					require.NoError(t, err)
				})
			},
		)
	})
}

func TestBucketPutIfRangeOverwrite(t *testing.T) {
	// Key 重名 PutIf 保持第一个值
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				for j := 0; j < iterCount; j++ {
					_ = bucket.PutIf(keyNum(0), valNum(j))
				}
			})
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				j := 0
				err := bucket.Range(func(key, val Bytes) {
					j++
					require.Equal(t, valNum(0), val.B())
				})
				require.Equal(t, 1, j)
				require.NoError(t, err)
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						_ = bucket.PutIf(keyNum(0), valNum(j))
					}
				})
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					j := 0
					err := bucket.Range(func(key, val Bytes) {
						j++
						require.Equal(t, valNum(0), val.B())
					})
					require.Equal(t, 1, j)
					require.NoError(t, err)
				})
			},
		)
	})
}

func TestBucketDel(t *testing.T) {
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			// Put
			runParallel(bucketCount, func(i int) {
				bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
			})
			// Del
			require.Equal(t, int64(2), db.Stats().DiskSegment)
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				for j := 0; j < iterCount; j++ {
					// 只保留 [20, 30]
					if j <= 30 && j >= 20 {
						continue
					}
					_ = bucket.Del(keyNum(j))
				}
			})
			// Get
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				val, err := bucket.Get(keyNum(30))
				require.NoError(t, err)
				require.Equal(t, valNum(30), val.B())

				val, err = bucket.Get(keyNum(31))
				require.NoError(t, err)
				require.Nil(t, val)
			})
			// Range
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				j := 30
				err := bucket.Range(func(key, val Bytes) {
					require.Equal(t, string(keyNum(j)), key.String())
					require.Equal(t, string(valNum(j)), val.String())
					j--
				})
				require.Equal(t, 19, j)
				require.NoError(t, err)
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				// Put
				runParallel(bucketCount, func(i int) {
					bucketPut(iterCount, db.GetOrCreateBucket(bucketNum(i)))
				})
				// Del
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					for j := 0; j < iterCount; j++ {
						// 只保留 [20, 30]
						if j <= 30 && j >= 20 {
							continue
						}
						_ = bucket.Del(keyNum(j))
					}
				})
				require.Equal(t, int64(4), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)

				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				// Get
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					val, err := bucket.Get(keyNum(30))
					require.NoError(t, err)
					require.Equal(t, valNum(30), val.B())

					val, err = bucket.Get(keyNum(31))
					require.NoError(t, err)
					require.Nil(t, val)
				})
				// Range
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					j := 30
					err := bucket.Range(func(key, val Bytes) {
						require.Equal(t, string(keyNum(j)), key.String())
						require.Equal(t, string(valNum(j)), val.String())
						j--
					})
					require.Equal(t, 19, j)
					require.NoError(t, err)
				})
			},
		)
	})
}

func TestBucketClear(t *testing.T) {
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				bucketPut(iterCount, bucket)

				bucket.Clear()
				require.Equal(t, 0, bucket.Count())

				n := 0
				err := bucket.Range(func(key, val Bytes) {
					n++
				})
				require.NoError(t, err)
				require.Equal(t, 0, n)
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					bucketPut(iterCount, bucket)

					bucket.Clear()
					require.Equal(t, 0, bucket.Count())
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
				cpm, err := db.Compact()
				require.True(t, cpm)
				require.NoError(t, err)

				db.Gc()
				require.Equal(t, int64(1), db.Stats().DiskSegment)
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					require.Len(t, db.Buckets(), 0)
				})
				require.Equal(t, int64(2), db.Stats().DiskSegment)
			},
		)
	})
}

func TestBucketClearThenPut(t *testing.T) {
	opt := DefaultOptions()
	opt.MaxMemSegmentBytes = maxMemorySize

	t.Run("Open", func(t *testing.T) {
		runGrogudbTest(t, &opt, func(t require.TestingT, db *DB) {
			runParallel(bucketCount, func(i int) {
				bucket := db.GetOrCreateBucket(bucketNum(i))
				bucketPut(iterCount, bucket)

				bucket.Clear()
				require.NoError(t, bucket.Put(keyNum(1), valNum(1)))
				require.Equal(t, 1, bucket.Count())

				val, err := bucket.Get(keyNum(1))
				require.NoError(t, err)
				require.Equal(t, valNum(1), val.B())
			})
		}, nil)
	})

	t.Run("ReOpen", func(t *testing.T) {
		runGrogudbTest(t, &opt,
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					bucketPut(iterCount, bucket)
					bucket.Clear()
					require.NoError(t, bucket.Put(keyNum(1), valNum(1)))
				})
			},
			func(t require.TestingT, db *DB) {
				runParallel(bucketCount, func(i int) {
					bucket := db.GetOrCreateBucket(bucketNum(i))
					require.Equal(t, 1, bucket.Count())
					val, err := bucket.Get(keyNum(1))
					require.NoError(t, err)
					require.Equal(t, valNum(1), val.B())
				})
			},
		)
	})
}

// ----- Benchmark -----

const (
	benchmarkIter    = 100000 // 100k
	benchmarkBuckets = 100
)

func BenchmarkBucketPut(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkBucketPut(b)
	}
}

func benchmarkBucketPut(b *testing.B) {
	runGrogudbTest(b, nil, func(t require.TestingT, db *DB) {
		start := time.Now()
		wg := sync.WaitGroup{}
		for i := 0; i < benchmarkBuckets; i++ {
			wg.Add(1)
			n := i
			go func() {
				defer wg.Done()
				bucket := db.GetOrCreateBucket(bucketNum(n))
				for j := 0; j < benchmarkIter; j++ {
					_ = bucket.Put(keyNum(j), valNum(j))
				}
			}()
		}
		wg.Wait()

		since := time.Since(start)
		ops := float64(benchmarkIter*benchmarkBuckets) / since.Seconds()
		b.Logf("Benchmark Put elapsed[%s]: iter=%d, bucket=%d, ops=%f, stat=%+v", since, benchmarkIter, benchmarkBuckets, ops, db.Stats())
	}, nil)
}

func BenchmarkBucketPutIf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkBucketPutIf(b)
	}
}

func benchmarkBucketPutIf(b *testing.B) {
	runGrogudbTest(b, nil, func(t require.TestingT, db *DB) {
		start := time.Now()
		wg := sync.WaitGroup{}
		for i := 0; i < benchmarkBuckets; i++ {
			wg.Add(1)
			n := i
			go func() {
				defer wg.Done()
				bucket := db.GetOrCreateBucket(bucketNum(n))
				for j := 0; j < benchmarkIter; j++ {
					_ = bucket.PutIf(keyNum(0), valNum(j))
				}
			}()
		}
		wg.Wait()

		since := time.Since(start)
		ops := float64(benchmarkIter*benchmarkBuckets) / since.Seconds()
		b.Logf("Benchmark PutIf elapsed[%s]: iter=%d, bucket=%d, ops=%f, stat=%+v", since, benchmarkIter, benchmarkBuckets, ops, db.Stats())
	}, nil)
}

func BenchmarkBucketRange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkBucketRange(b)
	}
}

func benchmarkBucketRange(b *testing.B) {
	runGrogudbTest(b, nil, func(t require.TestingT, db *DB) {
		wg := sync.WaitGroup{}
		for i := 0; i < benchmarkBuckets; i++ {
			wg.Add(1)
			n := i
			go func() {
				defer wg.Done()
				bucket := db.GetOrCreateBucket(bucketNum(n))
				for j := 0; j < benchmarkIter; j++ {
					_ = bucket.Put(keyNum(j), valNum(j))
				}
			}()
		}
		wg.Wait()

		start := time.Now()
		wg = sync.WaitGroup{}
		var total atomic.Int64
		for i := 0; i < benchmarkBuckets; i++ {
			wg.Add(1)
			n := i
			go func() {
				defer wg.Done()

				bucket := db.GetOrCreateBucket(bucketNum(n))
				err := bucket.Range(func(key, val Bytes) {
					// drains
					total.Add(1)
				})
				require.NoError(t, err)
			}()
		}
		wg.Wait()

		since := time.Since(start)
		require.Equal(t, int64(benchmarkIter*benchmarkBuckets), total.Load())
		ops := float64(benchmarkIter*benchmarkBuckets) / since.Seconds()
		b.Logf("Benchmark Range elapsed[%s]: iter=%d, bucket=%d, ops=%f, stat=%+v", since, benchmarkIter, benchmarkBuckets, ops, db.Stats())
	}, nil)
}

func BenchmarkBucketHas(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkBucketHas(b)
	}
}

func benchmarkBucketHas(b *testing.B) {
	runGrogudbTest(b, nil, func(t require.TestingT, db *DB) {
		wg := sync.WaitGroup{}
		for i := 0; i < benchmarkBuckets; i++ {
			wg.Add(1)
			n := i
			go func() {
				defer wg.Done()
				bucket := db.GetOrCreateBucket(bucketNum(n))
				for j := 0; j < benchmarkIter; j++ {
					_ = bucket.Put(keyNum(j), valNum(j))
				}
			}()
		}
		wg.Wait()

		start := time.Now()
		var total int
		for i := 0; i < benchmarkBuckets; i++ {
			bucket := db.GetOrCreateBucket(bucketNum(i))
			for j := 0; j < benchmarkIter; j++ {
				if bucket.Has(keyNum(j)) {
					total++
				}
			}
		}
		require.Equal(t, benchmarkIter*benchmarkBuckets, total)

		since := time.Since(start)
		ops := float64(benchmarkIter*benchmarkBuckets) / since.Seconds()
		b.Logf("Benchmark Has elapsed[%s]: iter=%d, bucket=%d, ops=%f, stat=%+v", since, benchmarkIter, benchmarkBuckets, ops, db.Stats())
	}, nil)
}

func BenchmarkBucketDel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkBucketDel(b)
	}
}

func benchmarkBucketDel(b *testing.B) {
	runGrogudbTest(b, nil, func(t require.TestingT, db *DB) {
		wg := sync.WaitGroup{}
		for i := 0; i < benchmarkBuckets; i++ {
			wg.Add(1)
			n := i
			go func() {
				defer wg.Done()
				bucket := db.GetOrCreateBucket(bucketNum(n))
				for j := 0; j < benchmarkIter; j++ {
					_ = bucket.Put(keyNum(j), valNum(j))
				}
			}()
		}
		wg.Wait()

		start := time.Now()
		for i := 0; i < benchmarkBuckets; i++ {
			bucket := db.GetOrCreateBucket(bucketNum(i))
			for j := 0; j < benchmarkIter; j++ {
				_ = bucket.Del(keyNum(j))
			}
		}

		since := time.Since(start)
		ops := float64(benchmarkIter*benchmarkBuckets) / since.Seconds()
		b.Logf("Benchmark Del elapsed[%s]: iter=%d, bucket=%d, ops=%f, stat=%+v", since, benchmarkIter, benchmarkBuckets, ops, db.Stats())
	}, nil)
}
