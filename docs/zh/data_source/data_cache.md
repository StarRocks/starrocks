---
displayed_sidebar: docs
---

# Data Cache

本文介绍 Data Cache 的原理，以及如何开启 Data Cache 加速外部数据查询。自 v3.3.0 起，Data Cache 功能默认开启。

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

开启 Data Cache 后，StarRocks 会缓存从外部存储系统读取的数据文件。

## 缓存介质

StarRocks 以 BE 节点的内存和磁盘作为缓存的存储介质，支持全内存缓存或者内存+磁盘的两级缓存。
注意，当使用磁盘作为缓存介质时，缓存加速效果和磁盘本身性能直接相关，建议使用高性能本地磁盘（如本地 NVMe 盘）进行数据缓存。如果磁盘本身性能一般，也可通过增加多块盘来减少单盘 I/O 压力。

## 缓存淘汰机制

在 Data Cache 中，StarRocks 采用 [LRU](https://baike.baidu.com/item/LRU/1269842) (least recently used) 策略来缓存和淘汰数据，大致如下：

- 优先从内存读取数据，如果在内存中没有找到再从磁盘上读取。从磁盘上读取的数据，会尝试加载到内存中。
- 从内存中淘汰的数据，会尝试写入磁盘；从磁盘上淘汰的数据，会被废弃。

## 开启 Data Cache

自 v3.3.0 起，Data Cache 功能默认开启。

默认情况下，系统会通过以下方式缓存数据：

- 系统变量 `enable_scan_datacache` 和 BE 参数 `datacache_enable` 默认设置为 `true`。
- 如未手动配置缓存路径和内存以及磁盘上限，系统会自动选择相应的路径并设置上限：
  - 在 `storage_root_path` 目录下创建 **datacache** 目录作为磁盘缓存目录。（您可以通过 BE 参数 `datacache_disk_path` 修改。）
  - 开启磁盘空间自动调整功能。根据缓存磁盘当前使用情况自动设置上限，保证当前缓存盘整体磁盘使用率在 70% 左右，并根据后续磁盘使用情况动态调整。（您可以通过 BE 参数 `datacache_disk_high_level`、`datacache_disk_safe_level` 以及 `datacache_disk_low_level` 调整该行为。）
  - 默认配置缓存数据的内存上限为 `0`。（您可以通过 BE 参数 `datacache_mem_size` 修改。）
- 默认使用异步缓存方式，减少缓存填充影响数据读操作。
- 默认启用 I/O 自适应功能，当磁盘 I/O 负载比较高时，系统会自动将一部分请求路由到远端存储，减少磁盘压力。

如需禁用 Data Cache，需要执行以下命令：

```SQL
SET GLOBAL enable_scan_datacache=false;
```

## 填充 Data Cache

### 填充规则

自 v3.3.2 起，为了提高 Data Cache 的缓存命中率，StarRocks 按照如下规则填充 Data Cache：

- 对于非 SELECT 的查询， 不进行填充，比如 `ANALYZE TABLE`，`INSERT INTO SELECT` 等。
- 扫描一个表的所有分区时，不进行填充。但如果该表仅有一个分区，默认进行填充。
- 扫描一个表的所有列时，不进行填充。但如果该表仅有一个列，默认进行填充。
- 对于非 Hive、Paimon、Delta Lake、Hudi 或 Iceberg 的表，不进行填充。

您可以通过 `EXPLAIN VERBOSE` 命令查看指定查询的具体填充行为。

示例：

```sql
mysql> explain verbose select col1 from hudi_table;
|   0:HudiScanNode                        |
|      TABLE: hudi_table                  |
|      partitions=3/3                     |
|      cardinality=9084                   |
|      avgRowSize=2.0                     |
|      dataCacheOptions={populate: false} |
|      cardinality: 9084                  |
+-----------------------------------------+
```

其中 `dataCacheOptions={populate: false}` 即表明不填充 Data Cache，因为该查询会扫描全部分区。

您还可以通过 Session Variable [populdate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode) 进一步精细化管理该行为。

### 填充方式

Data Cache 支持以同步或异步的方式进行缓存填充。

- 同步填充

  使用同步填充方式时，会将当前查询所读取的远端数据都缓存在本地。同步方式填充效率较高，但由于缓存填充操作在数据读取时执行，可能会对首次查询效率带来影响。

- 异步填充

  使用异步填充方式时，系统会尝试在尽可能不影响读取性能的前提下在后台对访问到的数据进行缓存。异步方式能够减少缓存填充对首次读取性能的影响，但填充效率较低。通常单次查询不能保证将访问到的所以数据都缓存到本地，往往需要多次。

自 v3.3.0 起，系统默认以异步方式进行缓存，您可以通过修改 Session 变量 [enable_datacache_async_populate_mode](../sql-reference/System_variable.md) 来修改填充方式。

## Footer Cache

除了支持对数据湖查询中涉及到的远端文件数据进行缓存外，StarRocks 还支持对文件解析后的部分元数据（Footer）进行缓存。Footer Cache 通过将解析后生成 Footer 对象直接缓存在内存中，在后续访问相同文件 Footer 时，可以直接从缓存中获得该对象句柄进行使用，避免进行重复解析。

当前 StarRocks 支持缓存 Parquet Footer 对象。

您可通过设置以下系统变量启用 Footer Cache：

```SQL
SET GLOBAL enable_file_metacache=true;
```

> **注意**
>
> Footer Cache 基于 Data Cache 的内存模块进行数据缓存，因此需要保证 BE 参数 `datacache_enable` 为 `true` 且为 `datacache_mem_size` 配置一个合理值。

## I/O 自适应

为了避免当缓存磁盘 I/O 负载过高时，磁盘访问出现明显长尾，导致访问缓存系统出现负优化，Data Cache 提供 I/O 自适应功能，用于在磁盘负载过高时将一部分缓存请求路由到远端存储，同时利用本地缓存和远端存储来提升 I/O 吞吐。该功能默认开启。

您可通过设置以下系统变量启用 I/O 自适应：

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

-- 调整所有 BE 实例的 Data Cache 磁盘上限。
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
datacache_auto_adjust_enable=true
```

开启自动扩缩容后：

- 当磁盘占用比例高于 BE 参数 `datacache_disk_high_level` 中规定的阈值（默认值 `80`, 即磁盘空间的 80%）时，系统自动淘汰缓存数据，释放磁盘空间。
- 当磁盘占用比例在一定时间内持续低于 BE 参数 `datacache_disk_low_level` 中规定的阈值（默认值 `60`, 即磁盘空间的 60%），且当前磁盘用于缓存数据的空间已经写满时，系统将自动进行缓存扩容，增加缓存上限。
- 当进行缓存自动扩容或缩容时，系统将以 BE 参数 `datacache_disk_safe_level` 中规定的阈值（默认值 `70`, 即磁盘空间的 70%）为目标，尽可能得调整缓存容量。

## 查看 Data Cache 命中情况

您可以在 query profile 里观测当前 query 的 cache 命中情况。观测下述三个指标查看 Data Cache 的命中情况：

- `DataCacheReadBytes`：从内存和磁盘中读取的数据量。
- `DataCacheWriteBytes`：从外部存储系统加载到内存和磁盘的数据量。
- `BytesRead`：总共读取的数据量，包括从内存、磁盘以及外部存储读取的数据量。

示例一：StarRocks 从外部存储系统中读取了大量的数据 (7.65 GB)，从内存和磁盘中读取的数据量 (518.73 MB) 较少，即代表 Data Cache 命中较少。

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

示例二：StarRocks 从 data cache 读取了 46.08 GB 数据，从外部存储系统直接读取的数据量为 0，即代表 data cache 完全命中。

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

## 相关参数

您可以通过以下系统变量和 BE 参数配置 Data Cache。

### 系统变量

- [populdate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode)
- [enable_datacache_io_adaptor](../sql-reference/System_variable.md#enable_datacache_io_adaptor)
- [enable_file_metacache](../sql-reference/System_variable.md#enable_file_metacache)
- [enable_datacache_async_populate_mode](../sql-reference/System_variable.md)

### BE 参数

- [datacache_enable](../administration/management/BE_configuration.md#datacache_enable)
- [datacache_disk_path](../administration/management/BE_configuration.md#datacache_disk_path)
- [datacache_meta_path](../administration/management/BE_configuration.md#datacache_meta_path)
- [datacache_mem_size](../administration/management/BE_configuration.md#datacache_mem_size)
- [datacache_disk_size](../administration/management/BE_configuration.md#datacache_disk_size)
- [datacache_auto_adjust_enable](../administration/management/BE_configuration.md#datacache_auto_adjust_enable)
- [datacache_disk_high_level](../administration/management/BE_configuration.md#datacache_disk_high_level)
- [datacache_disk_safe_level](../administration/management/BE_configuration.md#datacache_disk_safe_level)
- [datacache_disk_low_level](../administration/management/BE_configuration.md#datacache_disk_low_level)
- [datacache_disk_adjust_interval_seconds](../administration/management/BE_configuration.md#datacache_disk_adjust_interval_seconds)
- [datacache_disk_idle_seconds_for_expansion](../administration/management/BE_configuration.md#datacache_disk_idle_seconds_for_expansion)
- [datacache_min_disk_quota_for_adjustment](../administration/management/BE_configuration.md#datacache_min_disk_quota_for_adjustment)

