# grogudb

grogudb 是一个为高频 Put/Has/Del/Range 操作而设计的持久化 KV 数据库。

<img width="600px" alt="image" src="https://user-images.githubusercontent.com/19553554/230955362-226ec6e5-69c7-4824-88b1-c56bf32ff3e0.png">

## Features

* 纯 Go 实现，可内嵌进程序中。
* 高效的 Put/Has/Del/Range 操作。
* 线程安全。
* 允许存储超过物理内存的数据。
* 简洁的 API。

设计文档详见 [Design Documention](./docs/design.md)

## Usages

### DB 操作

DB 配置项

```golang
// Options 控制 DB 行为的配置项
type Options struct {
	// MaxMemSegmentBytes memory segment 最大允许字节
	MaxMemSegmentBytes int

	// MaxDiskSegmentBytes disk segment 最大允许字节
	MaxDiskSegmentBytes int

	// CompactFragmentation compact 碎片比例
	// 即超过 HoldBytes 中需要被删除的字节数超过此比例时才需要 compact
	CompactFragmentation float64

	// CompactCheckInterval compact 巡检周期
	CompactCheckInterval time.Duration

	// CompactForceInterval 强制 compact 周期 即 compact 兜底行为 确保该周期内一定会执行一次 compact
	// CompactForceInterval 必须大于 CompactCheckInterval
	// 扫描磁盘是有 IO 开销 force 只是会忽略所有前置判断条件进行扫描 不代表会执行 compact 操作
	CompactForceInterval time.Duration

	// CompactKeyOpDelta compact 时会对两个 CompactCheckInterval 周期的 Put/Del 做差值计算
	// 超过一定差值才会进行 compact 目的是为了尽量减少 compact 操作
	CompactKeyOpDelta int

	// GcInterval gc 巡检周期
	GcInterval time.Duration
}
```

打开 DB

```golang
package main

import "github.com/chenjiandongx/grogudb"

func main() {
	db, err := grogudb.Open("/path/to/db", nil)
	if err != nil {
		// handle err
	}
	defer db.Close()
}
```

### Bucket 写操作

```golang
bucket := db.GetOrCreateBucket("bucket0")

// Clear 清空 Bucket 所有 key
func (b *Bucket) Clear()

// Put 新增 Key/Value 记录
func (b *Bucket) Put(key, val []byte) error

// PutIf 当 Key 不存在的时候设置 Key/Value Key 存在时不做操作
func (b *Bucket) PutIf(key, val []byte) error

// Del 删除指定 Key
func (b *Bucket) Del(key []byte) error
```

### Bucket 读操作

```golang
bucket := db.GetOrCreateBucket("bucket0")

// Has 判断 Key 是否存在
func (b *Bucket) Has(key []byte) bool

// Count 返回 Bucket Keys 数量
func (b *Bucket) Count() int

// Get 返回指定 Key 对应的 Value
//
// Get 返回的数据不允许直接修改，如果有修改需求 请使用 .Copy() 复制后的数据
// Get 是一个开销`相对高昂`的操作，查询 key 有 3 种情况
//  1. key 不存在，直接返回，无 IO
//  2. key 存在，在 memory segment 中检索，key 命中，无 IO
//  3. key 存在，在 memory segment 未命中，退避到 disk segment 检索
//     由于 key 是没有排序的，因此必须按序扫描所有的 block 直至找到，此时会有读放大的情况（比如为了查找 10B 数据而扫描了 2MB 的 datablock）
//     同时 disk segment 的搜索已经做了一些措施来尽量避免陷入 IO，如提前判断 key 是否存在，bloomfilter 加速过滤...
func (b *Bucket) Get(key []byte) (Bytes, error)

// Range 遍历每个 Key 并执行 fn 方法
//
// Range 返回的数据不允许直接修改 如果有修改需求 请使用 .Copy() 复制后的数据
// 请勿在 Range 内调用 Bucket 其他 API 避免死锁
func (b *Bucket) Range(fn func(key, val Bytes)) error

// FastRange 拷贝 memory segment 元素并遍历每个 Key 并执行 fn 方法
//
// 避免长期占用锁影响写入 但同时会带来一定的内存开销
// Range 返回的数据不允许直接修改 如果有修改需求 请使用 .Copy() 复制后的数据
// 请勿在 Range 内调用 Bucket 其他 API 避免死锁
func (b *Bucket) FastRange(fn func(key, val Bytes)) error
```

## Benchmark

grogudb 并不为 `Get` 操作而设计，不进行极限的性能压测（详见设计文档）。

* 压测机器： **2019 MacBook Pro 12C/16G**。
* 压测 DB：[gorgudb](https://github.com/chenjiandongx/grogudb)、[leveldb](https://github.com/syndtr/goleveldb) 以及 [badger](https://github.com/dgraph-io/badger)。
* 压测项目： [chenjiandongx/grogudb-benchmark](https://github.com/chenjiandongx/grogudb-benchmark)

**除了 Get API，其他所有操作性能几乎均优于 badger/leveldb。**

Iter: 10k, Bucket: 100 => 1M key

```docs
Storage: grogudb    Op: PutUnique       Elapsed: 1.587680726s   Ops: 629849.555785/s
Storage: leveldb    Op: PutUnique       Elapsed: 3.207660071s   Ops: 311753.732586/s
Storage: badger     Op: PutUnique       Elapsed: 3.713279852s   Ops: 269303.699117/s
Storage: grogudb    Op: PutDuplicate    Elapsed: 809.645321ms   Ops: 1235108.724849/s
Storage: leveldb    Op: PutDuplicate    Elapsed: 3.344680804s   Ops: 298982.192502/s
Storage: badger     Op: PutDuplicate    Elapsed: 3.809289718s   Ops: 262516.131360/s
Storage: grogudb    Op: PutIf           Elapsed: 179.746041ms   Ops: 5563404.870764/s
Storage: leveldb    Op: PutIf           Elapsed: 512.160806ms   Ops: 1952511.766392/s
Storage: badger     Op: PutIf           Elapsed: 1.063730519s   Ops: 940087.721597/s
Storage: grogudb    Op: Has             Elapsed: 79.718185ms    Ops: 12544189.258699/s
Storage: leveldb    Op: Has             Elapsed: 1.188825549s   Ops: 841166.309759/s
Storage: badger     Op: Has             Elapsed: 1.443558895s   Ops: 692732.387618/s
Storage: grogudb    Op: Del             Elapsed: 1.25951208s    Ops: 793958.244529/s
Storage: leveldb    Op: Del             Elapsed: 3.471029382s   Ops: 288098.972940/s`
Storage: badger     Op: Del             Elapsed: 4.524956978s   Ops: 220996.576291/s
Storage: grogudb    Op: Range           Elapsed: 81.139301ms    Ops: 12.324484/s
Storage: leveldb    Op: Range           Elapsed: 71.821588ms    Ops: 13.923390/s
Storage: badger     Op: Range           Elapsed: 295.666737ms   Ops: 3.382186/s
Storage: grogudb    Op: Get             Elapsed: 26.561270284s  Ops: 37648.801782/s
Storage: leveldb    Op: Get             Elapsed: 1.080395935s   Ops: 925586.599879/s
Storage: badger     Op: Get             Elapsed: 1.423728937s   Ops: 702380.891483/s
```

Iter: 100k, Bucket: 100 => 10M key (Without grogudb)

```docs
Storage: grogudb    Op: PutUnique       Elapsed: 16.423032579s  Ops: 608900.941522/s
Storage: leveldb    Op: PutUnique       Elapsed: 51.516953146s  Ops: 194110.858452/s
Storage: badger     Op: PutUnique       Elapsed: 42.421363992s  Ops: 235730.279721/s
Storage: grogudb    Op: PutDuplicate    Elapsed: 8.815478924s   Ops: 1134368.318070/s
Storage: leveldb    Op: PutDuplicate    Elapsed: 39.615313747s  Ops: 252427.636037/s
Storage: badger     Op: PutDuplicate    Elapsed: 47.31107471s   Ops: 211367.001517/s
Storage: grogudb    Op: PutIf           Elapsed: 2.299923889s   Ops: 4347969.968844/s
Storage: leveldb    Op: PutIf           Elapsed: 5.870490731s   Ops: 1703435.105892/s
Storage: badger     Op: PutIf           Elapsed: 15.958825217s  Ops: 626612.539709/s
Storage: grogudb    Op: Has             Elapsed: 850.056456ms   Ops: 11763924.536325/s
Storage: leveldb    Op: Has             Elapsed: 19.188154981s  Ops: 521154.848390/s
Storage: badger     Op: Has             Elapsed: 22.721393642s  Ops: 440113.848541/s
Storage: grogudb    Op: Del             Elapsed: 12.924122561s  Ops: 773746.918044/s
Storage: leveldb    Op: Del             Elapsed: 44.487984603s  Ops: 224779.793673/s
Storage: badger     Op: Del             Elapsed: 44.315291044s  Ops: 225655.744652/s
Storage: grogudb    Op: Range           Elapsed: 524.850653ms   Ops: 1.905304/s
Storage: leveldb    Op: Range           Elapsed: 1.177148523s   Ops: 0.849510/s
Storage: badger     Op: Range           Elapsed: 3.15658723s    Ops: 0.316798/s
```

## Contribution

**PRs always welcome.**

欢迎对此项目感兴趣的开发者参与到开发和讨论中来。

## License

Apache License v2 [©chenjiandongx](https://github.com/chenjiandongx)
