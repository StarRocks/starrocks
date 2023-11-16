---
displayed_sidebar: "Chinese"
---

# Data Cache

本文介绍 Data Cache 的原理，以及如何开启 Data Cache 加速外部数据查询。

在数据湖分析场景中，StarRocks 作为 OLAP 查询引擎需要扫描 HDFS 或对象存储（下文简称为“外部存储系统”）上的数据文件。查询实际读取的文件数量越多，I/O 开销也就越大。此外，在即席查询 (ad-hoc) 场景中，如果频繁访问相同数据，还会带来重复的 I/O 开销。

为了进一步提升该场景下的查询性能，StarRocks 2.5 版本开始提供 Data Cache 功能。通过将外部存储系统的原始数据按照一定策略切分成多个 block 后，缓存至 StarRocks 的本地 BE 节点，从而避免重复的远端数据拉取开销，实现热点数据查询分析性能的进一步提升。Data Cache 仅在使用外部表（不含 JDBC 外部表）和使用 External Catalog 查询外部存储系统中的数据时生效，在查询 StarRocks 原生表时不生效。

## 原理

StarRocks 将远端存储文件缓存至本地 BE 节点时，会将原始文件按照一定策略切分为相等大小的 block。block 是数据缓存的最小单元，大小可配置。当配置 block 大小为 1 MB 时，如果查询 Amazon S3 上一个 128 MB 的 Parquet 文件，StarRocks 会按照 1 MB 的步长，将该文件拆分成相等的 128 个 block，即 [0, 1 MB)、[1 MB, 2 MB)、[2 MB, 3 MB) ... [127 MB, 128 MB)，并为每个 block 分配一个全局唯一 ID，即 cache key。Cache key 由三部分组成。

```Plain
hash(filename) + fileModificationTime + blockId
```

说明如下。

| **组成项** | **说明**                                                     |
| ---------- | ------------------------------------------------------------ |
| filename   | 数据文件名称。                                               |
| fileModificationTime  | 数据文件最近一次修改时间。                                    |
| blockId    | StarRocks 在拆分数据文件时为每个 block 分配的 ID。该 ID 在一个文件下是唯一的，非全局唯一。 |

假如该查询命中了 [1 MB, 2 MB) 这个 block，那么：

1. StarRocks 检查缓存中是否存在该 block。
2. 如存在，则从缓存中读取该 block；如不存在，则从 Amazon S3 远端读取该 block 并将其缓存在 BE 上。

开启 Data Cache 后，StarRocks 会缓存从外部存储系统读取的数据文件。如不希望缓存某些数据，可进行如下设置。

```SQL
SET enable_populate_block_cache = false;
```

关于 `enable_populate_block_cache` 的更多信息，参见 [系统变量](../reference/System_variable.md#支持的变量)。

## 缓存介质

StarRocks 以 BE 节点的内存和磁盘作为缓存的存储介质，支持全内存缓存或者内存+磁盘的两级缓存。
注意，当使用磁盘作为缓存介质时，缓存加速效果和磁盘本身性能直接相关，建议使用高性能本地磁盘（如本地 NVMe 盘）进行数据缓存。如果磁盘本身性能一般，也可通过增加多块盘来减少单盘 I/O 压力。

## 缓存淘汰机制

在 Data Cache 中，StarRocks 采用 [LRU](https://baike.baidu.com/item/LRU/1269842) (least recently used) 策略来缓存和淘汰数据，大致如下：

- 优先从内存读取数据，如果在内存中没有找到再从磁盘上读取。从磁盘上读取的数据，会尝试加载到内存中。
- 从内存中淘汰的数据，会尝试写入磁盘；从磁盘上淘汰的数据，会被废弃。

## 开启 Data Cache

Data Cache 默认关闭。如要启用，则需要在 FE 和 BE 中同时进行如下配置。

### FE 配置

支持使用以下方式在 FE 中开启 Data Cache：

- 按需在单个会话中开启 Data Cache。

  ```SQL
  SET enable_scan_block_cache = true;
  ```

- 为当前所有会话开启全局 Data Cache。

  ```SQL
  SET GLOBAL enable_scan_block_cache = true;
  ```

### BE 配置

在每个 BE 的 **conf/be.conf** 文件中增加如下参数。添加后，需重启每个 BE 让配置生效。

| **参数**               | **说明**                                                     |**默认值** |
| ---------------------- | ------------------------------------------------------------ |----------|
| block_cache_enable     | 是否启用 Data Cache。<ul><li>`true`：启用。</li><li>`false`：不启用。</li></ul>| false |
| block_cache_disk_path  | 磁盘路径。支持添加多个路径，多个路径之间使用分号(;) 隔开。建议 BE 机器有几个磁盘即添加几个路径。BE 进程启动时会自动创建配置的磁盘缓存目录（当父目录不存在时创建失败）。 | `${STARROCKS_HOME}/block_cache` |
| block_cache_meta_path  | Block 的元数据存储目录，一般无需配置。 | `${STARROCKS_HOME}/block_cache` |
| block_cache_mem_size   | 内存缓存数据量的上限，单位：字节。推荐将该参数值最低设置成 20 GB。如在开启 Data Cache 期间，存在大量从磁盘读取数据的情况，可考虑调大该参数。 | 2147483648，即 2 GB |
| block_cache_disk_size  | 单个磁盘缓存数据量的上限，单位：字节。举例：在 `block_cache_disk_path` 中配置了 2 个磁盘，并设置 `block_cache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。| 0 表示仅使用内存作为缓存介质，不使用磁盘。 |

示例如下：

```Plain
# 开启 Data Cache。
block_cache_enable = true  

# 设置磁盘路径，假设 BE 机器有两块磁盘。
block_cache_disk_path = /home/disk1/sr/dla_cache_data/;/home/disk2/sr/dla_cache_data/ 

# 设置内存缓存数据量的上限为 2 GB。
block_cache_mem_size = 2147483648

# 设置单个磁盘缓存数据量的上限为 1.2 TB。
block_cache_disk_size = 1288490188800
```

## 查看 Data Cache 命中情况

您可以在 query profile 里观测当前 query 的 cache 命中情况。观测下述三个指标查看 Data Cache 的命中情况：

- `BlockCacheReadBytes`：从内存和磁盘中读取的数据量。
- `BlockCacheWriteBytes`：从外部存储系统加载到内存和磁盘的数据量。
- `BytesRead`：总共读取的数据量，包括从内存、磁盘以及外部存储读取的数据量。

示例一：StarRocks 从外部存储系统中读取了大量的数据 (7.65 GB)，从内存和磁盘中读取的数据量 (518.73 MB) 较少，即代表 Data Cache 命中较少。

```Plain
 - Table: lineorder
 - BlockCacheReadBytes: 518.73 MB
   - __MAX_OF_BlockCacheReadBytes: 4.73 MB
   - __MIN_OF_BlockCacheReadBytes: 16.00 KB
 - BlockCacheReadCounter: 684
   - __MAX_OF_BlockCacheReadCounter: 4
   - __MIN_OF_BlockCacheReadCounter: 0
 - BlockCacheReadTimer: 737.357us
 - BlockCacheWriteBytes: 7.65 GB
   - __MAX_OF_BlockCacheWriteBytes: 64.39 MB
   - __MIN_OF_BlockCacheWriteBytes: 0.00 
 - BlockCacheWriteCounter: 7.887K (7887)
   - __MAX_OF_BlockCacheWriteCounter: 65
   - __MIN_OF_BlockCacheWriteCounter: 0
 - BlockCacheWriteTimer: 23.467ms
   - __MAX_OF_BlockCacheWriteTimer: 62.280ms
   - __MIN_OF_BlockCacheWriteTimer: 0ns
 - BufferUnplugCount: 15
   - __MAX_OF_BufferUnplugCount: 2
   - __MIN_OF_BufferUnplugCount: 0
 - BytesRead: 7.65 GB
   - __MAX_OF_BytesRead: 64.39 MB
   - __MIN_OF_BytesRead: 0.00
```

示例二：StarRocks 从 data cache 读取了 46.08 GB 数据，从外部存储系统直接读取的数据量为 0，即代表 data cache 完全命中。

```Plain
 Table: lineitem
- BlockCacheReadBytes: 46.08 GB
 - __MAX_OF_BlockCacheReadBytes: 194.99 MB
 - __MIN_OF_BlockCacheReadBytes: 81.25 MB
- BlockCacheReadCounter: 72.237K (72237)
 - __MAX_OF_BlockCacheReadCounter: 299
 - __MIN_OF_BlockCacheReadCounter: 118
- BlockCacheReadTimer: 856.481ms
 - __MAX_OF_BlockCacheReadTimer: 1s547ms
 - __MIN_OF_BlockCacheReadTimer: 261.824ms
- BlockCacheWriteBytes: 0.00 
- BlockCacheWriteCounter: 0
- BlockCacheWriteTimer: 0ns
- BufferUnplugCount: 1.231K (1231)
 - __MAX_OF_BufferUnplugCount: 81
 - __MIN_OF_BufferUnplugCount: 35
- BytesRead: 46.08 GB
 - __MAX_OF_BytesRead: 194.99 MB
 - __MIN_OF_BytesRead: 81.25 MB
```
