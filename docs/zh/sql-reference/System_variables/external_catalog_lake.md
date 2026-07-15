---
displayed_sidebar: docs
sidebar_label: "外部 Catalog 与数据湖"
sidebar_position: 6
description: "用于外部 Catalog 连接器以及 Hive、Iceberg、Hudi 等数据湖格式的会话变量。"
---

# 系统变量 - 外部 Catalog 与数据湖

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### avro_use_jni_reader

* **作用域**: Session
* **描述**: 控制 StarRocks 在扫描 Hive 等外部 Catalog 中的 Avro 数据时，是否使用基于 JNI 的 Avro Reader。启用后（`true`），StarRocks 会使用 JNI Reader；关闭后（`false`），StarRocks 会使用原生 Avro Reader。当前该变量主要用于兼容性兜底。该变量默认关闭，因此默认会使用原生 Avro Reader。

  当前说明：
  - 原生 Avro Reader 与 JNI Reader 在 `CHAR(n)` 语义上已经对齐。相关对齐见 [#73579](https://github.com/StarRocks/starrocks/pull/73579)，因此当前 native 与 JNI 行为在这一点上保持一致。
  - 原生 Avro Reader 目前仅支持 `null`、`deflate` 和 `snappy` 这几种 codec，不支持 `bzip2` 等其他 codec。如果需要处理原生 Reader 不支持的 codec，请手动启用 JNI Reader。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v4.1.1

### connector_io_tasks_per_scan_operator

* 描述：外表查询时每个 Scan 算子能同时下发的 I/O 任务的最大数量。目前外表查询时会使用自适应算法来调整并发 I/O 任务的数量，通过 `enable_connector_adaptive_io_tasks` 开关来控制，默认打开。
* 默认值：16
* 类型：Int
* 引入版本：v2.5

### connector_sink_compression_codec

* 描述：用于指定写入 Hive 表或 Iceberg 表时以及使用 Files() 导出数据时的压缩算法。有效值：`uncompressed`、`snappy`、`lz4`、`zstd`、`gzip`。该参数只在以下情况生效：
  * Hive 表中未指定 `compression_codec` 属性。
  * Iceberg 表中未包含`write.parquet.compression-codec` 属性。
  * `INSERT INTO FILES` 时未设置 `compression` 属性。
* 默认值：uncompressed
* 类型：String
* 引入版本：v3.2.3

### connector_sink_target_max_file_size

* 描述: 指定将数据写入 Hive 表或 Iceberg 表或使用 Files() 导出数据时目标文件的最大大小。该限制并不一定精确，只作为尽可能的保证。
* 单位：Bytes
* 默认值: 1073741824
* 类型: Long
* 引入版本: v3.3.0

### enable_connector_adaptive_io_tasks

* 描述：外表查询时是否使用自适应策略来调整 I/O 任务的并发数。默认打开。如果未开启自适应策略，可以通过 `connector_io_tasks_per_scan_operator` 变量来手动设置外表查询时的 I/O 任务并发数。
* 默认值：true
* 引入版本：v2.5

### enable_write_hive_external_table

* 描述：是否开启往 Hive 的 External Table 写数据的功能。
* 默认值：false
* 引入版本：v3.2

### lake_bucket_assign_mode

* 描述：数据湖表查询的分桶分配模式。此变量控制系统执行查询期间启用 Bucket-aware 执行时如何将分桶分配给工作节点。有效值：
  * `balance`：在工作节点间均匀分配分桶以实现负载均衡，以获取更好的性能。
  * `elastic`：使用一致性哈希将分桶分配给工作节点，可以在弹性环境中提供更好的负载分配。
* 默认值：balance
* 数据类型：String
* 引入版本：v4.0

### metadata_collect_query_timeout

* 描述：Iceberg Catalog 元数据收集阶段的超时时间。
* 单位： 秒
* 默认值：60
* 引入版本：v3.3.3

### orc_use_column_names

* 描述：设置通过 Hive Catalog 读取 ORC 文件时，列的对应方式。默认值是 `false`，即按照 Hive 表中列的顺序对应。如果设置为 `true`，则按照列名称对应。
* 引入版本：v3.1.10

### allow_lake_without_partition_filter

* 描述：是否允许对湖仓表（Hive、Iceberg、Delta Lake、Paimon 等）进行无分区过滤条件的查询。当设置为 `false` 时，未包含有效分区过滤条件的查询将被拒绝，以防止意外的全表扫描。
* 作用域：Session
* 默认值：`true`
* 数据类型：Boolean
* 别名：`allow_hive_without_partition_filter`

### scan_lake_partition_num_limit

* 描述：单张湖仓表（Hive、Iceberg、Delta Lake、Paimon 等）允许扫描的最大分区数。设置为 `0` 表示无限制。超出限制时查询将报错。注意：对于增量式枚举分片的 catalog 类型（Iceberg、Delta Lake），分区数限制在 scan-range 分发阶段检查，查询可能在执行中途失败而非被立即拒绝。
* 作用域：Session
* 默认值：`0`（无限制）
* 数据类型：Int
* 别名：`scan_hive_partition_num_limit`

