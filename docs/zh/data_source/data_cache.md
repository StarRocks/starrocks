---
displayed_sidebar: docs
---

# Data Cache

本文将介绍 Data Cache 的核心原理，以及如何通过开启 Data Cache 功能加速数据查询。

Data Cache 是用于缓存内表与外表数据。该功能自 v3.3.0 版本起默认开启，而从 V4.0 版本开始，内表与外表的内存缓存、磁盘缓存已统一纳入 Data Cache 体系进行管理。

Data Cache 包含两部分：Page Cache（内存缓存）与 Block Cache（磁盘缓存）。

## Page Cache 原理

Page Cache 作为内存级缓存，负责存储内表与外表解压锁后的数据页，Page 其大小不固定。目前，Page Cache 支持缓存以下几类数据：

1. 内表的数据页和索引页
2. 外表数据文件的 Footer 信息
3. 外表解压缩后的部分数据页

Page Cache 目前采用 LRU（最近最少使用）策略进行数据淘汰。

## Block Cache 原理

Block Cache 则为磁盘缓存，主要作用是将外表及存算分离场景下存储在远程对象存储中的数据文件，缓存到本地磁盘，以此降低远端数据访问延迟、提升查询效率，Block 大小固定。

### 功能背景与价值

在数据湖分析或存算分离内表场景中，StarRocks 作为 OLAP 查询引擎需扫描 HDFS 或对象存储（下文统一简称 “外部存储系统”）中的数据文件。该过程存在两大性能瓶颈：

1. 查询需读取的文件数量越多，远端 I/O 开销越大。
2. 在即席查询（ad-hoc）场景中，频繁访问相同数据会产生重复的远端 I/O 消耗。

为解决上述问题，StarRocks 从 2.5 版本起引入 Block Cache 功能：将外部存储系统的原始数据按特定策略切分为多个 block，缓存至 StarRocks 本地 BE 节点。通过避免重复拉取远端数据，显著提升热点数据的查询分析性能。

### 生效场景

- 使用外部表（不含 JDBC 外部表）查询外部存储系统数据时。
- 查询存算分离模式下的 StarRocks 原生表时。

### 核心工作机制

**数据切分与缓存单元**

StarRocks 缓存远端文件时，会将原始文件按配置策略切分为等大小的 block（block 是缓存的最小单元，大小可自定义）。

- 示例：若配置 block 大小为 1 MB，查询 Amazon S3 上 128 MB 的 Parquet 文件时，文件会被拆分为 128 个连续 block（如 [0, 1 MB)、[1 MB, 2 MB) … [127 MB, 128 MB)）；
  每个 block 会分配一个全局唯一的缓存标识（cache key），由以下三部分组成：
```Plain
hash(filename) + fileModificationTime + blockId
```

说明如下：

| **组成项** | **说明**                                                     |
| ---------- | ------------------------------------------------------------ |
| filename   | 数据文件名称。                                               |
| fileModificationTime  | 数据文件最近一次修改时间。                                    |
| blockId    | StarRocks 在拆分数据文件时为每个 block 分配的 ID。该 ID 在一个文件下是唯一的，非全局唯一。 |

**缓存命中与读取流程**

以查询命中 [1 MB, 2 MB) 区间的 block 为例，流程如下：

1. StarRocks 先检查本地 BE 节点的 Block Cache 中是否存在该 block（通过 cache key 匹配）。
2. 若存在（命中缓存），直接从本地磁盘读取该 block。
3. 若不存在（未命中），从远端存储（如 Amazon S3）拉取该 block，并同步缓存到本地 BE 节点，供后续查询复用。

### 缓存介质

Block Cache 以 BE 节点的本地磁盘作为存储介质，缓存加速效果与磁盘性能直接相关：

- 推荐使用高性能本地磁盘（如 NVMe 盘），最大化降低缓存读写延迟。
- 若磁盘性能一般，可通过增加磁盘数量实现负载分担，减少单盘 I/O 压力。

### 缓存淘汰机制

Block Cache 支持两种数据缓存与淘汰策略，默认使用 SLRU（Segmented LRU） 策略：

|策略|核心逻辑|优势|
|----|----|----|
|LRU|基于 “最近最少使用” 原则，淘汰最长时间未被访问的 block。|实现简单，适用于访问模式稳定的场景。|
|SLRU|将缓存空间划分为淘汰段和保护段，两段均遵循 LRU 规则：<br>1. 数据首次访问时进入淘汰段；<br>2. 淘汰段数据被再次访问时，升级至保护段；<br>3. 保护段数据淘汰时回退至淘汰段，淘汰段数据淘汰时直接移出缓存。| 能有效抵御突发稀疏流量，避免 “仅访问一次的临时数据” 直接淘汰保护段的热点数据，稳定性优于 LRU。|

## 开启 Data Cache

Data Cache 功能默认开启，通过 BE 节点的配置参数实现，可开关和控制资源上限，具体说明如下：

### 开关参数

1. 总开关：控制 Data Cache 整体开启/关闭

- 参数名：datacache_enable
- 作用：Data Cache 功能的总开关，直接控制 Page Cache 与 Block Cache 的整体状态。
- 取值说明：
    - true（默认）：Data Cache 整体开启，Page Cache 和 Block Cache 可通过各自子开关独立控制；
    - false：Data Cache 整体关闭，Page Cache 和 Block Cache 均失效。

2. 子缓存开关：独立控制 Page Cache/Block Cache

| 子缓存类型       | 参数名                        | 作用                | 取值说明                   |
|-------------|----------------------------|-------------------|------------------------|
| Page Cache  | disable_storage_page_cache | 控制 Page Cache 启停  | false（默认，开启）；true（关闭）  |
| Block Cache | block_cache_enable         | 控制 Block Cache 启停 | true（默认，开启）；false（关闭）｜ |

### 资源上限配置参数

通过以下 BE 参数设置 Data Cache 的内存与磁盘使用上限，避免资源过度占用：

- datacache_mem_size：设置 Data Cache 的内存使用上限（用于 Page Cache 缓存数据）
- datacache_disk_size：设置 Data Cache 的磁盘使用上限（用于 Block Cache 缓存数据）

## 填充 Block Cache

### 填充规则

自 v3.3.2 版本起，为进一步提升 Block Cache 的命中率，StarRocks 按以下规则判断是否填充缓存，未满足条件的场景将跳过填充：

1. 仅对 SELECT 查询 进行填充，非 SELECT 操作（如 ANALYZE TABLE、INSERT INTO SELECT 等）不填充；
2. 扫描表的所有分区时不填充，但若表仅含 1 个分区，默认填充；
3. 扫描表的所有列时不填充，但若表仅含 1 个列，默认填充；
4. 仅对 Hive、Paimon、Delta Lake、Hudi、Iceberg 类型的表 进行填充，其他类型表不填充。

### 查看缓存填充行为

可通过 `EXPLAIN VERBOSE` 命令，查看指定查询的具体缓存填充策略，关键信息通过 dataCacheOptions 字段体现：

示例：

```sql
mysql> explain verbose select col1 from hudi_table;
....
|   0:HudiScanNode                        |
|      TABLE: hudi_table                  |
|      partitions=3/3                     |
|      cardinality=9084                   |
|      avgRowSize=2.0                     |
|      dataCacheOptions={populate: false} |
|      cardinality: 9084                  |
+-----------------------------------------+
```

如上示例中，因查询扫描表的全部分区，dataCacheOptions={populate: false} 表明该查询不会触发 Data Cache 填充。

您还可以通过 Session Variable [populate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode) 对缓存填充规则进行更灵活的自定义配置。

### 填充方式

Block Cache 支持 同步填充 和 异步填充 两种模式，可根据业务对 “首次查询性能” 和 “缓存效率” 的需求选择：

|填充方式|核心逻辑| 优缺点对比                                                                    |
|----|----|--------------------------------------------------------------------------|
|同步填充|首次查询读取远端数据时，立即将数据缓存到本地，后续查询可直接复用缓存。| 优点：缓存效率高，单次查询即可完成数据缓存；<br> 缺点：缓存操作与读取操作同步执行，可能增加首次查询延迟。                  |
|异步填充|首次查询仅优先完成数据读取，后台异步执行缓存写入，不阻塞当前查询流程。| 优点：不影响首次查询性能，避免读取操作因缓存而延迟；<br> 缺点：缓存效率较低，单次查询可能无法完全缓存所有访问数据，需多次查询逐步完善缓存。 |

自 v3.3.0 起，系统默认以异步方式进行缓存，您可以通过修改 Session 变量 [enable_datacache_async_populate_mode](../sql-reference/System_variable.md) 来修改填充方式。

### 持久化

Block Cache 当前默认会持久化磁盘缓存数据，BE 进程重启后，可直接复用先前磁盘缓存数据。

## 查看 Block Cache 命中情况

可通过 Query Profile（查询性能剖析）观测当前查询的 Block Cache 命中效果，核心关注以下三个关键指标，通过指标间的数值对比即可判断命中情况：

- `DataCacheReadBytes`：从内存和磁盘中读取的数据量。
- `DataCacheWriteBytes`：从外部存储系统加载到内存和磁盘的数据量。
- `BytesRead`：总共读取的数据量，包括从内存、磁盘以及外部存储读取的数据量。

示例一：StarRocks 从外部存储系统中读取了大量的数据 (7.65 GB)，从内存和磁盘中读取的数据量 (518.73 MB) 较少，即代表 Block Cache 命中较少。

```Plain
 - Table: lineorder
 - DataCacheReadBytes: 518.73 MB
   - __MAX_OF_DataCacheReadBytes: 4.73 MB
   - __MIN_OF_DataCacheReadBytes: 16.00 KB
 - DataCacheReadCounter: 684
   - __MAX_OF_DataCacheReadCounter: 4
   - __MIN_OF_DataCacheReadCounter: 0
 - DataCacheReadTimer: 737.357us
 - DataCacheWriteBytes: 7.65 GB
   - __MAX_OF_DataCacheWriteBytes: 64.39 MB
   - __MIN_OF_DataCacheWriteBytes: 0.00 
 - DataCacheWriteCounter: 7.887K (7887)
   - __MAX_OF_DataCacheWriteCounter: 65
   - __MIN_OF_DataCacheWriteCounter: 0
 - DataCacheWriteTimer: 23.467ms
   - __MAX_OF_DataCacheWriteTimer: 62.280ms
   - __MIN_OF_DataCacheWriteTimer: 0ns
 - BufferUnplugCount: 15
   - __MAX_OF_BufferUnplugCount: 2
   - __MIN_OF_BufferUnplugCount: 0
 - BytesRead: 7.65 GB
   - __MAX_OF_BytesRead: 64.39 MB
   - __MIN_OF_BytesRead: 0.00
```

示例二：StarRocks 从 Block Cache 读取了 46.08 GB 数据，从外部存储系统直接读取的数据量为 0，即代表 Block Cache 完全命中。

```Plain
 Table: lineitem
- DataCacheReadBytes: 46.08 GB
 - __MAX_OF_DataCacheReadBytes: 194.99 MB
 - __MIN_OF_DataCacheReadBytes: 81.25 MB
- DataCacheReadCounter: 72.237K (72237)
 - __MAX_OF_DataCacheReadCounter: 299
 - __MIN_OF_DataCacheReadCounter: 118
- DataCacheReadTimer: 856.481ms
 - __MAX_OF_DataCacheReadTimer: 1s547ms
 - __MIN_OF_DataCacheReadTimer: 261.824ms
- DataCacheWriteBytes: 0.00 
- DataCacheWriteCounter: 0
- DataCacheWriteTimer: 0ns
- BufferUnplugCount: 1.231K (1231)
 - __MAX_OF_BufferUnplugCount: 81
 - __MIN_OF_BufferUnplugCount: 35
- BytesRead: 46.08 GB
 - __MAX_OF_BytesRead: 194.99 MB
 - __MIN_OF_BytesRead: 81.25 MB
```

## I/O 自适应

为避免缓存磁盘 I/O 负载过高时，出现磁盘访问长尾问题（进而导致缓存系统产生负优化），Data Cache 提供 I/O 自适应功能。其核心逻辑是：当磁盘负载超出阈值时，自动将部分缓存请求路由至远端存储，通过 “本地缓存 + 远端存储” 的协同访问，平衡 I/O 压力、提升整体 I/O 吞吐。
该功能默认处于开启状态，无需额外配置即可生效。若此前手动关闭，可通过以下系统变量重新启用：

```SQL
SET GLOBAL enable_datacache_io_adaptor=true;
```

## 在线扩缩容

Data Cache 支持在不重启 BE 进程的情况下对缓存容量进行手动调整，并支持对磁盘空间的自动调整。

### 手动扩缩容

您可以通过动态修改 BE 配置项来修改缓存的内存或磁盘上限。

示例：

```SQL
-- 调整特定 BE 实例的 Data Cache 内存上限。
UPDATE be_configs SET VALUE="10G" WHERE NAME="datacache_mem_size" and BE_ID=10005;

-- 调整所有 BE 实例的 Data Cache 内存比例上限。
UPDATE be_configs SET VALUE="10%" WHERE NAME="datacache_mem_size";

-- 调整所有 BE 实例的 Data Cache 单个磁盘上限。
UPDATE be_configs SET VALUE="2T" WHERE NAME="datacache_disk_size";
```

> **注意**
>
> - 通过以上方式进行容量调整时一定要谨慎，注意不要遗漏 WHERE 字句避免修改其他配置项。
> - 通过以上方式在线调整的缓存容量不会被持久化，待 BE 进程重启后会失效。因此，您可以先通过以上方式在线调整参数，再手动修改 BE 配置文件，保证下次重启后修改依然生效。

### 自动扩缩容

当前系统支持磁盘空间的自动调整。如果您未在 BE 配置中指定缓存磁盘路径和上限时，系统默认打开自动扩缩容功能。

您也可通过在 BE 配置文件中添加以下配置项并重启 BE 进程来打开自动扩缩容功能：

```Plain
enable_datacache_disk_auto_adjust=true
```

开启自动扩缩容后：

- 当磁盘占用比例高于 BE 参数 `disk_high_level` 中规定的阈值（默认值 `90`, 即磁盘空间的 90%）时，系统自动淘汰缓存数据，释放磁盘空间。
- 当磁盘占用比例在一定时间内持续低于 BE 参数 `disk_low_level` 中规定的阈值（默认值 `60`, 即磁盘空间的 60%），且当前磁盘用于缓存数据的空间已经写满时，系统将自动进行缓存扩容，增加缓存上限。
- 当进行缓存自动扩容或缩容时，系统将以 BE 参数 `disk_safe_level` 中规定的阈值（默认值 `80`, 即磁盘空间的 80%）为目标，尽可能得调整缓存容量。

## Cache Sharing

由于 Block Cache 依赖于 BE 节点的本地磁盘，当集群进行扩展时，数据路由的变化可能会导致 Cache Miss，这很容易在弹性扩展过程中导致明显的性能抖动。

Cache Sharing 能够通过网络来访问集群中其他节点上的缓存数据。在集群扩展过程中，如果发生本地 Cache Miss，系统会首先尝试从同一集群内的其他节点获取缓存数据。只有当所有缓存都未命中时，系统才会从远程存储中重新获取数据。这一功能可有效减少弹性扩展过程中缓存失效造成的性能抖动，并确保稳定的查询性能。

![cache sharing workflow](../_assets/cache_sharing_workflow.png)

您可以通过配置以下两个项目启用缓存共享功能：

- 将 FE 配置项 `enable_trace_historical_node` 设为 `true`。
- 将系统变量 `enable_datacache_sharing` 设置为 `true`。

此外，还可以在查询配置文件中检查以下指标，以监控缓存共享：

- `DataCacheReadPeerCounter`：从其他缓存节点读取的次数。
- `DataCacheReadPeerBytes`：从其他缓存节点读取的字节数。
- `DataCacheReadPeerTimer`：从其他节点访问缓存数据所用的时间。

## 相关参数

您可以通过以下系统变量和参数配置 Data Cache。

### 系统变量

- [populate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode)
- [enable_datacache_io_adaptor](../sql-reference/System_variable.md#enable_datacache_io_adaptor)
- [enable_file_metacache](../sql-reference/System_variable.md#enable_file_metacache)
- [enable_datacache_async_populate_mode](../sql-reference/System_variable.md)
- [enable_datacache_sharing](../sql-reference/System_variable.md#enable_datacache_sharing)

### FE 参数

- [enable_trace_historical_node](../administration/management/FE_configuration.md#enable_trace_historical_node)

### BE 参数

- [datacache_enable](../administration/management/BE_configuration.md#datacache_enable)
- [datacache_mem_size](../administration/management/BE_configuration.md#datacache_mem_size)
- [datacache_disk_size](../administration/management/BE_configuration.md#datacache_disk_size)
- [datacache_auto_adjust_enable](../administration/management/BE_configuration.md#datacache_auto_adjust_enable)
- [datacache_disk_high_level](../administration/management/BE_configuration.md#datacache_disk_high_level)
- [datacache_disk_safe_level](../administration/management/BE_configuration.md#datacache_disk_safe_level)
- [datacache_disk_low_level](../administration/management/BE_configuration.md#datacache_disk_low_level)
- [datacache_disk_adjust_interval_seconds](../administration/management/BE_configuration.md#datacache_disk_adjust_interval_seconds)
- [datacache_disk_idle_seconds_for_expansion](../administration/management/BE_configuration.md#datacache_disk_idle_seconds_for_expansion)
- [datacache_min_disk_quota_for_adjustment](../administration/management/BE_configuration.md#datacache_min_disk_quota_for_adjustment)
- [datacache_eviction_policy](../administration/management/BE_configuration.md#datacache_eviction_policy)
- [datacache_inline_item_count_limit](../administration/management/BE_configuration.md#datacache_inline_item_count_limit)
