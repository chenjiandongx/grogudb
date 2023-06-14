# Design Documention

grogudb 是一个为高频 Put/Has/Del/Range 操作而设计的持久化 KV 数据库。

## Overview

grogudb 设计初衷是为了顺序读写的场景，在调研了开源社区成熟的持久化 KV 存储后，发现大多数项目都是考虑了更通用的场景，如：

* 读写均衡
* 批量写
* 事务性
* 断电恢复
* ...

没有发现对高频 `Put/Has/Del/Range` 场景优化设计的存储（甚至可以没有 `Get` 操作）。假设 KV DB 也有 Table 的概念，在这里称为 Bucket，那对于 DB 来讲，实际上使用就是一种「垂直写，水平查」的行为。

**多 Buckets 同时垂直写，单 Bucket 水平读。**

```docs
Buckets
  ^   
  │   . . . . . . . . . . . . . . . . .   . . . . .   
  │     . . . . . . . . . . . . . . . . . . . . . .   Range: Bucket01
  │         . . . . . . .
  │       . . .     . . . . . . . . . . . . . . . .                  ... 
  │     . . . . . . . . . . . . . . . . .   . . . .   
  │     . . . . . . . . . .   . . . . . . . . . . .   Range: Bucket99
  │           . . .   . . . . . . . . .   . . . . .   Range: BucketN ...
  │         . . . . . . . . .       . . . . .
  │       . . .     . . . . . . . . . . . . . . . .                  ... 
  │     . . . . . . . . . . . . . . . .   . . . . 
  v
    <----------------- KV Record ------------------>
```

这种用法符合 [LSM-Tree](https://www.wikiwand.com/en/Log-structured_merge-tree) 的设计思路，把**逻辑上相邻**的数据在**物理落盘**上也使其相邻，即顺序读写。这样在减少随机 IO 带来的开销的同时还能利用 [CPU SIMD](https://www.wikiwand.com/en/Single_instruction,_multiple_data) 特性。

具体做法是将数据分为热区（memory segment）以及冷区（disk segment）再通过层级 compact 来归档数据，同时利用 WAL 来保证断电恢复，不少**追求吞吐**的 DB 都使用了这种类似的实现方式。

但仔细思考一些问题：

**1. WAL 能 100% 保证断电恢复吗？**

**不是的。** 严格意义上讲，于操作系统而言，并不是每次 `write syscall` 都会将数据落盘，而是会写到内核缓存区，批量写入，除非每次 `write` 之后都调用 `fsync` 刷盘，但这显然非常影响效率。

**2. 事务性是强要求吗？**

**看情况。** 取决于业务需求，不一定所有的业务都要求在一个 Txn 里同时进行 write/read 操作，因此可允许提供单操作的原子性即可。

**3. 读写性能需要同时保证吗？**

**不一定。** 上层需求决定底层设计，比如业务只需要 `Put/Del/Range` 操作，根本没有 `Get` 要求，这也是写这个项目的缘由。所以可以在设计上牺牲一部分的查询性能来换取写入性能。

经过一番思考和取舍后，设计就逐渐明朗，我们要什么？

1. 设计简单，可维护性好。
2. 对 `Write/Scan` 操作进行性能优化。
3. 不需要严格的数据完整性，允许断电故障。
4. 操作并发安全，但不提供事务。

bitcask 提供了一种非常轻巧的设计方案，详见论文 [bitcask-intro](https://riak.com/assets/bitcask-intro.pdf)，本项目也是参考该论文实现的。

## Key Hash

每次写入 record 时会将 key 做 hash(uint64)，并将 hash 记录至对应的 bucket 中，即每个 bucket 持有自己的 `key hashmap`。这种做法使得 DB 可以掌握全局所有 bucket keys 情况。

* 优点：`has` 操作非常快，几乎为 O(1)，且没有任何 IO。
* 缺点：`hash map` 会占用一定的内存（map 扩容）。

## Segments

### Memory Segment

数据存储的切割单位为 `segment`，所有数据写入是先落入 `memory segment` 中，待数据写满缓冲区后（默认 8MB）Flush 至磁盘，并归档为 `disk segment`。

```docs
+ --- Head ----- | --------------- | --------------- | --- | --------------- +
| Memory Segment | Disk Segment(1) | Disk Segment(2) | ... | Disk Segment(N) | 
+ -------------- | --------------- | --------------- | --- | --------------- +
```

memory segment 由多个 buckets 组成。bucket 是为了 shard 而设计的接口，类比于 MySQL 中的 Table 概念（参考了 bblot 设计）。

`bucket buffer` 为双向链表设计，使用双向链表而不是切片数组的原因如下：

1. 内存优化，没有扩容开销，无需预留 buffer。
2. 删除操作开销低，解链即可。
3. 更新数据开销低，元素解引用并重新链至末尾即可。

缺点是元素使用指针相链，会带来一定的 GC 扫描开销。

```docs
                      + ------------- +
DoublyLinkedList -->  | *head         |
                      | ------------- |
                      | key/record(1) |
                      | ------------- |
                      | key/record(2) |   
                      | ------------- |                            
                      | key/record(3) |
                      | ------------- |
                      | .......       |
                      | ------------- |                               
                      | key/record(N) |
                      + ------------- +
```

当 buffer 满时进行归档，归档操作如下，此过程保证线程安全：

1. 反转列表，因为数据是按时间序排列的（oldest -> newest），但读取时是需要从最新数据往回读的（newest -> oldest）。假设 record(1) 先增后删，按序遍历的话是 put -> del，此时可能会误判为该 key 存在。而反转以后，如果确定最新数据已经是删除状态，则可以直接返回。
2. 对归档数据写入 checksum，保证落盘数据完整性。
3. 将 memory segment 转换为 disk segment，同时置空 memory segment。
4. 状态标记。

memory segment 保证了单个 segment 内不会同时存在两条相同 key 的 record 记录。

### Disk Segment

disk segment 包含两个文件，命名规则为 `data_$segid`，`keys_$data`，前者保存了完整的 record 信息，后者保存了 key 信息。

*DataFile 布局：*

#### DataFile

DataFile 数据以二进制存储，分为 4 块内容。

```docs
+ ----------- | --------- | ----------- | ----------- +
| DataBlock   | MetaBlock | BloomFilter | Footer      |
+ ----------- | --------- | ----------- | ----------- +
```

下面逐一介绍。

**DataBlock**

DataBlock 由多条 record 一起组成，布局如下：

```docs
+ --------- +
| Record(1) |
+ --------- +
| Record(2) |
+ --------- +
| ...       |
+ --------- +
| Record(N) |
+ --------- +
| Checksum  |
+ --------- +
```

*Record 布局：*

```docs
+ -------------- | -------- | ----------- | --- | ------------- | ----- +
| RecordSize(4B) | Flag(1B) | KeySize(4B) | Key | ValueSize(4B) | Value |
+ -------------- | -------- | ----------- | --- | ------------- | ----- +
```

* RecordSize: record 大小，即 [Flag, Value] 区间大小。
* Flag: 数据标识，Put/Del/Tombstone
* KeySize/Key: key 大小以及内容。
* ValueSize/Value: value 大小以及内容。

**MetaBlock**

*Metadata 布局：*

```docs
+ -------------- | ------ | ------------------ | -------------- | ----------------- | ------------ + 
| BucketSize(4B) | Bucket | RecordPosCount(4B) | RecordPosBlock | KeyEntityPosBlock | Checksum(4B) |
+ -------------- | ------ | ------------------ | -------------- | ----------------- | ------------ + 

RecordPosBlock/KeyEntityPosBlock

+ ----------- +
| Position(1) |
+ ----------- +
| Position(2) |
+ ----------- +
| ...         |
+ ----------- +
| Position(N) |
+ ----------- +

Position

+ ----------------- | --------------- +
| PositionStart(4B) | PositionEnd(4B) |
+ ----------------- | --------------- +
```

* BucketSize/Bucket: bucket name 大小及其内容。
* RecordPosCount/RecordPosBlock: record position 数量，一个 position 描述了数据块的起始和终止位置。比如单个 bucket 在 disk segment 中有 20M 的数据，那其将会被切割成若干个小块，每个小块对应着各自的 position（读取更高效）。
* KeyEntityPosCount/KeyEntityPosBlock: 同 record pos，只不过描述的是 key 的存储。
* Checksum: 校验码。

**BloomFilter**

*BloomFilter 布局：*

```docs
+ ---------- +
| BloomBytes |
+ ---------- +
```

* BloomBytes: 布隆过滤器实现。

**Footer**

*Footer 布局：*

```docs
+ ------------ | ------------ | -------------------- | -------------------- | --------- +
| DataSize(4B) | MetaSize(4B) | BloomFilterCount(4B) | BloomFilterBytes(4B) | Magic(4B) |
+ ------------ | ------------ | -------------------- | -------------------- | --------- +
```

* DataSize: 数据块大小。
* MetaSize: 元数据块大小。
* BloomFilterCount: bloomFilter 元素个数。
* BloomFilterBytes: bloomFilter 字节数组。
* Magic: 魔法数。

#### KeysFile

Keys 由多个 keyEntity 组成，布局如下：

```docs
+ ------------ +
| KeyEntity(1) |
+ ------------ +
| KeyEntity(2) |
+ ------------ +
| ...          |
+ ------------ +
| KeyEntity(N) |
+ ------------ +
```

*KeyEntity 布局：*

```docs
+ -------- | ----------- | -------------- +
| Flag(1B) | KeyHash(8B) | RecordSize(4B) |
+ -------- | ----------- | -------------- +
```

* Flag: 数据标识，Put/Del/Tombstone
* KeyHash: key uint64 hash。
* RecordSize: record 大小。

关于 Keys 文件两个主要作用：

1. 启动扫描：启动时程序需要扫描所有的 disk segment 文件来还原 DB 关闭前的状态，使用 keys 文件可以减少读取数据量，因为此时并不需要扫描 key/val 具体内容，只要 key hash 即可。
2. Compact：compact 时也需要扫描所有 key，确定哪些 key 需要被合并或者被删除，同时还要判断变更的 record 大小。对于一个 disk segment，如果 compact 后只是将大小从 10M 压缩为 9.5M，那不具备 compact 效益，选择跳过。

## Compact

在 compact 的处理上，没有完全依照 level-compact，而是采用了一种平铺的方式，所有的 disk segment 都在同一个 level。

compact 的目的在于压缩磁盘空间，但同时会带来一定的 IO 开销，因此需要选择好 compact 的时机。默认的行为是当 compact 后的 disk segment 体积减小超过 50% 才进行操作。

compact 过程中对读写没有影响，每个 disk segment 会有引用计数，访问时 `ref++`，结束访问时 `ref--`，当且仅当 `ref == 0` 时才会删除磁盘文件，避免正在遍历的操作受影响。即程序实现了对于 disk segment 的 GC 行为。

```docs
| --------------- | --------------- | --------------- | --------------- |
| ref 0           | ref: 2          | ref: 0          | ref 0           |
| disk sgement(1) | disk sgement(2) | disk sgement(3) | disk sgement(4) |
| --------------- | --------------- | --------------- | --------------- |

---> Compact operation

| --------------- | --------------- | --------------- | --------------- |
| ref 0           | ref: 2          | ref: 0          | ref: 0          |
| disk sgement(1) | disk sgement(2) | disk sgement(3) | disk sgement(4) |
| --------------- | --------------- | --------------- | --------------- |
                  |       |                           |
                  | ------|-------------------------- |   // rollback if compact failed
                  |       |  new disk sgement(3)      |
                          |
                       Hanging: in database view, no more disk segment(2)   // lock guarded

                                | --------------- | --------------- | --------------- |
                                | ref 0           | ref: 0          | ref 0           |
                                | disk sgement(1) | disk sgement(3) | disk sgement(4) |
                                | --------------- | --------------- | --------------- |

                                After range/get opertion, disk segment(2) ref is reduced to 0.

                 | --------------- |
                 | ref: 0          |
                 | disk segment(2) |  GC worker will cleanup disk segment(2), remove all files of it.
                 | --------------- |
```

compact 操作启动后，会将现有的 disk segment 分成若干各组，分组规则为：

1. 单 disk segment 大小不得超过 MaxDiskSegmentBytes。
2. 相邻 disk segment 如果大小之和小于等于 MaxDiskSegmentBytes 则合并为一个分组。

```docs
// If MaxDiskSegmentBytes Option is 20MB.

| ---------- | ---------- | ---------- | ---------- | ---------- | ---------- |
| segment(1) | segment(2) | segment(3) | segment(4) | segment(5) | segment(6) |
| 10MB       | 9MB        | 18MB       | 10MB       | 7MB        | 2MB        |
| ---------- | ---------- | ---------- | ---------- | ---------- | ---------- |

// After grouping

| Group1                  | Group2     | Group3                               |
| ----------------------- | ---------- | ------------------------------------ |
| segment(1)   segment(2) | segment(3) | segment(4)   segment(5)   segment(6) |
| 19MB                    | 18MB       | 19MB                                 |
| ----------------------- | ---------- | ------------------------------------ |

// Compact

| ---------- | ---------- | ---------- |
| segment(2) | segment(3) | segment(6) |
| 19MB       | 18MB       | 19MB       |
| ---------- | ---------- | ---------- |
```

## Load

DB 启动时，如果路径下存在数据文件，会进行扫描和加载，对于 DataFile，解码顺序和编码是相反的。

1. 解码 Footer，并校验数据是否合法（Magic 判断）。
2. 解码 BloomFilter，校验 Checksum 并加载进内存中。
3. 解码 Metadata，校验 Checksum 并加载进内存中。
4. 解码 KeysFile，按序将 key 还原进内存中。

关于 BloomFilter，为了加速解压和节省内存空间使用，编码时是直接将 BloomFilter 二进制序列化写入 DataFile，解压时读取整片数据。

## Performance

grogudb 把 WAL 当成数据存储来使用，所以写入性能较为优异，写入的操作都是 Append-Only，而且对于磁盘交互部分，均使用了 BufferdIO 的思路，尽量减少 `write syscall` 次数。

不同操作性能开销：

1. Put: 如果某段时间窗口内对某个 key 存在高频的更新行为，实际上只有内存操作（解链然后重新链接到末尾），对比纯粹的 WAL 这种操作可以节约不少的磁盘 IO。
2. PutIf: 如果 key 已经存在，则是 O(1) 操作，不存在则跟 Put 行为保持一致。
3. Del: 追加 FlagDel 记录，并从内存中删除该 key 记录，其他跟 Put 操作无异。
4. Range: 按 block 顺序读取 record，使用 RecordRanger 解析数据流。
5. Has: O(1) 内存判断。
6. Clear: 追加 FlagTombstone 记录，从内存中删除该 bucket 所有 key，其他跟 Put 操作无异。

## Summary

编写 grogudb 主要是出于兴趣，想尝试更深地了解数据库的一些设计哲学。

毕竟古人有言，纸上得来终觉浅，绝知此事要躬行。
