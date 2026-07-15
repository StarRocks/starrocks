---
displayed_sidebar: docs
sidebar_label: "扫描、I/O 与 Data Cache"
sidebar_position: 5
description: "用于扫描并发、I/O 任务、Data Cache 以及页缓存的会话变量。"
---

# 系统变量 - 扫描、I/O 与 Data Cache

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### datacache_sharing_work_period

- 描述：Cache Sharing 功能的生效时长。每次群集扩展操作后，如果启用了缓存共享功能，只有在这段时间内的请求才会尝试访问其他节点的缓存数据。
- 默认值：600
- 单位：秒
- 引入版本：v3.5.1

### enable_datacache_async_populate_mode

* 描述：是否使用异步方式进行 Data Cache 填充。系统默认使用同步方式进行填充，即在查询数据时同步填充进行缓存填充。
* 默认值：false
* 引入版本：v3.2.7

### enable_datacache_io_adaptor

* 描述：是否开启 Data Cache I/O 自适应开关。`true` 表示开启。开启后，系统会根据当前磁盘 I/O 负载自动将一部分缓存请求路由到远端存储来减少磁盘压力。
* 默认值：true
* 引入版本：v3.3.0

### enable_datacache_sharing

- 描述：是否启用 Cache Sharing。设置为 `true` 可启用该功能。Cache Sharing 能够在本地缓存未命中时通过网络访问其他节点上的缓存数据，这有助于减少集群扩展过程中缓存失效造成的性能抖动。只有当 FE 参数 `enable_trace_historical_node` 设置为 `true` 时，此变量才会生效。
- 默认值：true
- 引入版本：v3.5.1

### enable_gin_filter

* 描述：查询时是否使用[全文倒排索引](../table_design/indexes/inverted_index.md)。
* 默认值：true
* 引入版本：v3.3.0

### enable_parquet_reader_bloom_filter

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否在读取 Parquet 文件时启用 Bloom Filter 优化。
  * `true`（默认）：读取 Parquet 文件时启用 Bloom Filter 优化。
  * `false`：读取 Parquet 文件时禁用 Bloom Filter 优化。
* 引入版本：v3.5.0

### enable_parquet_reader_page_index

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否在读取 Parquet 文件时启用 Page Index 优化。
  * `true`（默认）：读取 Parquet 文件时启用 Page Index 优化。
  * `false`：读取 Parquet 文件时禁用 Page Index 优化。
* 引入版本：v3.5.0

### enable_scan_datacache

* 描述：是否开启 Data Cache 特性。该特性开启之后，StarRocks 通过将外部存储系统中的热数据缓存成多个 block，加速数据查询和分析。更多信息，参见 [Data Cache](../data_source/data_cache.md)。该特性从 2.5 版本开始支持。在 3.2 之前各版本中，对应变量为 `enable_scan_block_cache`。
* 默认值：true
* 引入版本：v2.5

### io_tasks_per_scan_operator

* 每个 Scan 算子能同时下发的 I/0 任务的数量。如果使用远端存储系统（比如 HDFS 或 S3）且时延较长，可以增加该值。但是值过大会增加内存消耗。
* 默认值：4
* 类型：Int
* 引入版本：v2.5

### max_pushdown_conditions_per_column

* 描述：该变量的具体含义请参阅 [BE 配置项](../administration/management/BE_configuration.md)中 `max_pushdown_conditions_per_column` 的说明。
* 默认值：`-1`，表示使用 `be.conf` 中的配置值。如果设置大于 0，则忽略 `be.conf` 中的配置值。
* 类型：Int

### max_scan_key_num

* 描述：该变量的具体含义请参阅 [BE 配置项](../administration/management/BE_configuration.md)中 `max_scan_key_num` 的说明。
* 默认值：`-1`，表示使用 `be.conf` 中的配置值。如果设置大于 0，则忽略 `be.conf` 中的配置值。

### populate_datacache_mode

* 描述：StarRocks 从外部存储系统读取数据时，控制数据缓存填充行为。有效值包括：
  * `auto`（默认）：系统自动根据查询的特点，选择性进行缓存。
  * `always`：总是缓存数据。
  * `never` 永不缓存数据。
* 默认值：auto
* 引入版本：v3.3.2

### scan_olap_partition_num_limit

* 描述：在SQL执行计划中, 单表允许的最大扫描分区数.
* 默认值：0 (无限制)
* 引入版本：v3.3.9

### skip_local_disk_cache

* **描述**: 会话标志，指示 FE 在构建 scan ranges 时为每个 tablet 的内部扫描范围标记 `skip_disk_cache`。当设置为 `true` 时，`OlapScanNode.addScanRangeLocations()` 会在创建的 `TInternalScanRange` 对象上调用 `internalRange.setSkip_disk_cache(true)`，从而通知下游 BE 的扫描节点在该扫描中绕过本地磁盘缓存。该设置按会话应用，并在计划/扫描范围构建时评估。可与 `skip_page_cache`（控制页面缓存跳过）和数据缓存相关变量（`enable_scan_datacache` / `enable_populate_datacache`）结合使用。
* **作用域**: Session
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.3.9, v3.4.0, v3.5.0

