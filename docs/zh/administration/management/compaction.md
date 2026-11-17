---
sidebar_position: 120
displayed_sidebar: docs
description: "存算分离集群中 Compaction 的管理和监控方法。"
products: [oss, enterprise, anywhere]
---

# 存算分离集群 Compaction

本文介绍如何在 StarRocks 存算分离集群中管理 Compaction。

## 概述

在 StarRocks 中，每次导入都会生成一个新的版本。Compaction 将不同版本的数据文件合并为更大的文件，从而减少小文件的数量并提高查询效率。

## Compaction Score

### 介绍

Compaction Score 反映了分区的数据文件合并状态。分数越高，表示该分区的数据文件合并程度越低，即该分区有更多版本的数据文件需要合并。FE 为每个分区维护 Compaction Score 信息，包括 Max Compaction Score （即这个分区中 Compaction Score 最高的 Tablet 的分数）。当 Partition 的 Max Compaction Score 小于 FE 参数 `lake_compaction_score_selector_min_score`(默认10)，则该分区通常不再被选为自动 Compaction 候选，但这不表示删除标记已全部清理。当 Partition 的 Max Compaction Score 超过 100，就可以认为是不太健康的 Compaction 状态。当这个分区的 Max Compaction Score 超过 FE 参数 `lake_ingest_slowdown_threshold`(默认100)，系统会开始减缓这个分区的数据导入事务的提交速度，当超过 FE 参数 `lake_compaction_score_upper_bound`(默认2000) 时，系统会拒绝这个分区的数据导入事务。

### 计算规则

大多数情况下，一个数据文件对应的 Compaction Score 为 1，也就是说，假设一个分区只有一个 Tablet，如果第一次数据导入这个 Tablet 内产生了 10 个数据文件，那么这个分区的 Max Compaction Score 就是 10，并且一个事务在一个 Tablet 内产生的所有数据文件被称作是一个 Rowset。

除了以上的分数计算规则，实际的 Compaction Score 计算时还会把这个 Tablet 的所有 Rowset 按照大小规则进行分组，然后以 Score 最大的那一组作为其 Compaction Score。

假设一个 Tablet 经历了 7 次导入，生成了 7 个 Rowset，数据大小分别是 100 MB、100 MB、100 MB、10 MB、10 MB、10 MB、10 MB，那么计算时，首先会将其中三个 100 MB 分为一组，四个 10 MB 分为另一组，然后分别统计两个组内的数据文件数量，以数据文件多的那个组的 Compaction Score 作为这个 Tablet 的 Compaction Score。如果 Compaction Score 满足要求，后续进行 Compaction 时，会挑选分数最高的一组 Rowset 进行 Compaction。比如在这个例子里，如果是第二组 Compaction Score 更高，那么经过 Compaction，这个 Tablet 的 Rowset 分布会变成 100 MB、100 MB、100 MB、40 MB。

## 整体流程

与存算一体集群相比，存算分离集群引入了一种新的 FE 统一控制的 Compaction 调度机制，其流程是：

1. FE Leader 根据每个事务的 Publish 结果计算并存储对应 Partition 的 Compaction Score 信息。
2. FE 会按照 Partition 的 Max Compaction Score 选择分数最高的一批 Partition 作为 Compaction 任务的候选者。
3. FE 会依次对挑选出来的 Partition 开始 Compaction 事务， 生成对应的 Tablet 子任务并下发到 CN 上，直到子任务的数量到达 FE 参数 `lake_compaction_max_tasks` 的限制。
4. CN 会在后台以 Tablet 为单位执行 Compaction 子任务，并将结果返回给 FE。单个 CN 同时执行的子任务数量受 CN 参数 `compact_threads` 控制。
5. FE 收集所有子任务的结果，然后进行 Compaction 事务提交。
6. FE 将成功提交的 Compaction 事务进行 Publish。

## 主键表 Base Compaction {/* #primary-key-base-compaction */}

本节介绍存算分离主键表的 Base Compaction。普通 Cumulative Compaction 按 size-tiered 策略选取 Rowset，主要用于合并小文件。Base Compaction 将整个 Tablet 的 Rowset 纳入候选，优先重写删除行数较多的 Rowset，清理失效数据并缩小 Delete Vector。因此，在已有主键持续被更新或删除时，即使后台一直在合并小文件，也可能仍需要 Base Compaction。

### 版本支持 {/* #primary-key-base-compaction-versions */}

| 版本 | 自动 PK Base Compaction | 手动 PK Base Compaction | 存算分离 Base Compaction 时段和最小间隔控制 |
| --- | --- | --- | --- |
| v4.1.0～v4.1.3 | 不支持。 | 不支持 PK Base Compaction；仍可手动触发普通 Compaction。 | 不支持。 |
| v4.1.4～v4.1.6 | 调度后按删除比例或删除行数判断，没有独立的自动触发开关。 | 支持，不要求达到自动触发阈值。 | 不支持。 |
| v4.1.7～v4.1.8 | 由 `lake_pk_compaction_base_enable_delete_trigger` 控制，默认 `false`。 | 支持，不受该开关和删除阈值限制。 | 不支持。 |
| v4.2 | 由上述开关控制，默认 `false`。 | 支持，并可绕过自动 Base Compaction 的时段和最小间隔限制。 | 支持，详见[执行时间控制](#base-compaction-time-controls)。 |

PK Base Compaction 从 v4.1.4 开始支持。v4.1.7 新增的 `lake_pk_compaction_base_enable_delete_trigger` **只控制按删除量自动触发 Base Compaction**，不会禁止手动 Base Compaction，也不会关闭普通 Cumulative Compaction。

### 自动触发 {/* #automatic-base-compaction */}

自动 Compaction 仍由 FE 按分区调度。默认使用 Compaction Score 筛选候选分区；任务下发到 CN 后，CN 再决定某个 Tablet 是否执行 Base Compaction。达到删除阈值不会独立触发一次 FE 调度，因此不保证立即执行。

从 v4.1.7 开始，需要先将 CN 参数 `lake_pk_compaction_base_enable_delete_trigger` 设置为 `true`，再检查以下两个条件。v4.1.4～v4.1.6 没有此开关，直接检查这两个条件：

- Tablet 的整体删除比例 `sum(num_dels) / sum(num_rows)` 大于或等于 `lake_pk_compaction_base_delete_ratio_threshold`，默认 `0.5`。
- Tablet 的累计删除行数 `sum(num_dels)` 大于或等于 `lake_pk_compaction_base_delete_rows_threshold`，默认 `10000000`。

两个条件满足**任意一个**即可进入 Base Compaction 选取。统计范围为 Tablet 当前所有 Rowset；更新产生的旧行删除标记也计入。没有删除行时，继续使用普通 Compaction 选取。在支持[执行时间控制](#base-compaction-time-controls)的版本中，自动 Base Compaction 还必须满足相应的时段和最小间隔限制。

例如，从 v4.1.7 开始，可以动态开启删除量自动触发：

```sql
UPDATE information_schema.be_configs
SET VALUE = 'true'
WHERE NAME = 'lake_pk_compaction_base_enable_delete_trigger';
```

参数的完整定义见 [BE 参数参考](../configuration/BE_parameters/shared_lake_other.md#lake_pk_compaction_base_enable_delete_trigger)。在持续更新的大 Tablet 上，绝对删除行数可能很快再次达到阈值；开启前应评估反复重写数据的资源开销。

### 手动触发 {/* #manual-base-compaction */}

对目标表或分区执行 `ALTER TABLE ... COMPACT` 即可请求手动 Compaction，无需额外指定 `BASE`。SQL 示例见[手动触发 Compaction 任务](#手动触发-compaction-任务)。

- 从 v4.1.4 开始，Tablet 存在删除行时，手动请求可进入 Base Compaction 选取，无需达到上述两个删除阈值。
- 从 v4.1.7 开始，即使 `lake_pk_compaction_base_enable_delete_trigger = false`，仍可手动触发 Base Compaction。
- 在支持执行时间控制的版本中，手动请求还会绕过自动 Base Compaction 的禁用时段和最小间隔。
- Tablet 完全没有删除行时，会回退到普通 Compaction 选取，仍可能没有可合并的输入。

手动请求是异步的，SQL 返回成功表示请求已提交，不表示 Compaction 已完成。它仍受任务调度和资源限制，也不保证一次合完整个 Tablet。

### 输入范围与资源开销 {/* #base-compaction-inputs */}

Base Compaction 按各 Rowset 的**删除行数绝对值降序**选取输入，而不是按删除比例排序。所有 Rowset 都是候选，因此在预算允许时，无删除行的 Rowset 也可能被合并。

单次选取受 `lake_pk_compaction_max_input_rowsets`（默认 `500`）以及预计保留数据量预算 `max(update_compaction_result_bytes, Tablet 总数据量 × update_compaction_ratio_threshold)` 限制。后两个参数默认分别为 `1 GiB` 和 `0.5`。按 Rowset 选取时，最后一个 Rowset 可能使预计结果超过预算；该预算也不是实际读取字节数的上限。自动和手动 Base Compaction 都受这些输入限制，因此一次执行不保证清理全部删除标记。

### 执行时间控制 {/* #base-compaction-time-controls */}

以下能力从 v4.2 开始支持；**4.1 系列（包括当前 v4.1.8 分支）不支持这些存算分离限制**：

- 表属性 [`base_compaction_forbidden_time_ranges`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#禁止-base-compaction) 指定的是**禁止自动 Base Compaction** 的时段，其余时段允许。v4.1.6、v4.1.7 和当前 v4.1.8 分支的 FE 会拒绝为存算分离表设置此属性。
- CN 参数 [`base_compaction_interval_seconds_since_last_operation`](../configuration/BE_parameters/stats_storage.md#base_compaction_interval_seconds_since_last_operation) 默认 `86400` 秒，表示同一 Tablet 两次成功 Base Compaction 之间的自动选取最小间隔。它不是每天定时触发的周期；没有历史 Base Compaction 时间记录或该值小于等于 `0` 时，不施加间隔限制。此参数早于 v4.1 已存在，但在 4.1 系列（包括当前 v4.1.8 分支）中不用于限制存算分离 PK Base Compaction。

例如，仅在支持上述能力的版本中，可以禁止每天 08:00（含）到 21:00（不含）的自动 Base Compaction：

```sql
ALTER TABLE example_db.pk_table SET (
    "base_compaction_forbidden_time_ranges" = "* 8-20 * * *"
);
```

这些限制只控制自动 Base Compaction；普通 Cumulative Compaction 仍可继续，手动 `ALTER TABLE ... COMPACT` 可绕过这些限制。

### 查看执行效果 {/* #verify-base-compaction */}

使用[查看 Compaction 任务](#查看-compaction-任务)中的 `SHOW PROC '/compactions'` 和 `information_schema.be_cloud_native_compactions` 检查异步任务的状态和执行详情。Compaction Score 用于调度，不能单独用于判断删除标记是否已全部清理。

Base Compaction 会消耗 CN 的 CPU、内存以及对象存储读写资源。建议先选择一个分区手动执行并观察开销。Compaction 完成后，历史数据版本仍可能因保留策略而存在；旧版本的回收还受 AutoVacuum 和 [`lake_autovacuum_grace_period_minutes`](../configuration/FE_parameters/shared_lake_other.md#lake_autovacuum_grace_period_minutes) 控制，不能把任务完成等同于对象存储空间立即全部释放。

## 管理 Compaction

### 查看 Compaction Score

- 您可以通过 SHOW PROC 语句查看特定表中分区的 Compaction Score，通常只需要关注 `MaxCS` 字段，默认调度配置下，如果 `MaxCS` 低于 10，通常不再被选为自动 Compaction 候选，但不代表删除标记已全部清理；如果 `MaxCS` 大于 100，代表 Compaction Score 相对较高；如果 `MaxCS` 大于 500，代表 Compaction Score 非常高，需要人工介入。

  ```Plain
  SHOW PARTITIONS FROM <table_name>
  SHOW PROC '/dbs/<database_name>/<table_name>/partitions'
  ```

  示例：

  ```Plain
  mysql> SHOW PROC '/dbs/load_benchmark/store_sales/partitions';
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  | PartitionId | PartitionName | CompactVersion | VisibleVersion | NextVersion | State  | PartitionKey | Range | DistributionKey              | Buckets | DataSize | RowCount  | CacheTTL | AsyncWrite | AvgCS | P50CS | MaxCS |
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  | 38028       | store_sales   | 913            | 921            | 923         | NORMAL |              |       | ss_item_sk, ss_ticket_number | 64      | 15.6GB   | 273857126 | 2592000  | false      | 10.00 | 10.00 | 10.00 |
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  1 row in set (0.20 sec)
  ```

- 您也可以通过查询系统定义视图 `information_schema.partitions_meta` 查看分区的 Compaction Score。

  示例：

  ```Plain
  mysql> SELECT * FROM information_schema.partitions_meta ORDER BY Max_CS LIMIT 10;
  +--------------+----------------------------+----------------------------+--------------+-----------------+-----------------+----------------------+--------------+---------------+-----------------+-----------------------------------------+---------+-----------------+----------------+---------------------+-----------------------------+--------------+---------+-----------+------------+------------------+----------+--------+--------+-------------------------------------------------------------------+
  | DB_NAME      | TABLE_NAME                 | PARTITION_NAME             | PARTITION_ID | COMPACT_VERSION | VISIBLE_VERSION | VISIBLE_VERSION_TIME | NEXT_VERSION | PARTITION_KEY | PARTITION_VALUE | DISTRIBUTION_KEY                        | BUCKETS | REPLICATION_NUM | STORAGE_MEDIUM | COOLDOWN_TIME       | LAST_CONSISTENCY_CHECK_TIME | IS_IN_MEMORY | IS_TEMP | DATA_SIZE | ROW_COUNT  | ENABLE_DATACACHE | AVG_CS   | P50_CS | MAX_CS | STORAGE_PATH                                                      |
  +--------------+----------------------------+----------------------------+--------------+-----------------+-----------------+----------------------+--------------+---------------+-----------------+-----------------------------------------+---------+-----------------+----------------+---------------------+-----------------------------+--------------+---------+-----------+------------+------------------+----------+--------+--------+-------------------------------------------------------------------+
  | tpcds_1t     | call_center                | call_center                |        11905 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | cc_call_center_sk                       |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 12.3KB    |         42 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11906/11905 |
  | tpcds_1t     | web_returns                | web_returns                |        12030 |               3 |               3 | 2024-03-17 08:40:48  |            4 |               |                 | wr_item_sk, wr_order_number             |      16 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 3.5GB     |   71997522 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/12031/12030 |
  | tpcds_1t     | warehouse                  | warehouse                  |        11847 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | w_warehouse_sk                          |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 4.2KB     |         20 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11848/11847 |
  | tpcds_1t     | ship_mode                  | ship_mode                  |        11851 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | sm_ship_mode_sk                         |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 1.7KB     |         20 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11852/11851 |
  | tpcds_1t     | customer_address           | customer_address           |        11790 |               0 |               2 | 2024-03-17 08:32:19  |            3 |               |                 | ca_address_sk                           |      16 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 120.9MB   |    6000000 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11791/11790 |
  | tpcds_1t     | time_dim                   | time_dim                   |        11855 |               0 |               2 | 2024-03-17 08:30:48  |            3 |               |                 | t_time_sk                               |      16 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 864.7KB   |      86400 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11856/11855 |
  | tpcds_1t     | web_sales                  | web_sales                  |        12049 |               3 |               3 | 2024-03-17 10:14:20  |            4 |               |                 | ws_item_sk, ws_order_number             |     128 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 47.7GB    |  720000376 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/12050/12049 |
  | tpcds_1t     | store                      | store                      |        11901 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | s_store_sk                              |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 95.6KB    |       1002 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11902/11901 |
  | tpcds_1t     | web_site                   | web_site                   |        11928 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | web_site_sk                             |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 13.4KB    |         54 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11929/11928 |
  | tpcds_1t     | household_demographics     | household_demographics     |        11932 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | hd_demo_sk                              |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 2.1KB     |       7200 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11933/11932 |
  +--------------+----------------------------+----------------------------+--------------+-----------------+-----------------+----------------------+--------------+---------------+-----------------+-----------------------------------------+---------+-----------------+----------------+---------------------+-----------------------------+--------------+---------+-----------+------------+------------------+----------+--------+--------+-------------------------------------------------------------------+
  ```

### 查看 Compaction 任务

随着新数据导入系统，FE 会持续调度 Compaction 任务到不同的 CN 节点执行。您可以先查看 FE 上 Compaction 任务的总体状态，然后再查看 CN 上每个任务的执行详情。

#### 查看 Compaction 任务的总体状态

您可以通过 SHOW PROC 语句查看 Compaction 任务的总体状态。

```SQL
SHOW PROC '/compactions';
```

示例：

```Plain
mysql> SHOW PROC '/compactions';
+---------------------+-------+---------------------+---------------------+---------------------+-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Partition           | TxnID | StartTime           | CommitTime          | FinishTime          | Error | Profile                                                                                                                                                                                                              |
+---------------------+-------+---------------------+---------------------+---------------------+-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ssb.lineorder.10081 | 15    | 2026-01-10 03:29:07 | 2026-01-10 03:29:11 | 2026-01-10 03:29:12 | NULL  | {"sub_task_count":12,"read_local_sec":0,"read_local_mb":218,"read_remote_sec":0,"read_remote_mb":0,"read_segment_count":120,"write_segment_count":12,"write_segment_mb":219,"write_remote_sec":4,"in_queue_sec":18,"score_before":{"avg":10.0,"p50":10.0,"max":10.0},"score_after":{"avg":8.0,"p50":8.0,"max":8.0},"partial_success":false} |
| ssb.lineorder.10068 | 16    | 2026-01-10 03:29:07 | 2026-01-10 03:29:13 | 2026-01-10 03:29:14 | NULL  | {"sub_task_count":12,"read_local_sec":0,"read_local_mb":218,"read_remote_sec":0,"read_remote_mb":0,"read_segment_count":120,"write_segment_count":12,"write_segment_mb":218,"write_remote_sec":4,"in_queue_sec":38,"score_before":{"avg":10.0,"p50":10.0,"max":10.0},"score_after":{"avg":8.0,"p50":8.0,"max":8.0},"partial_success":false} |
| ssb.lineorder.10055 | 20    | 2026-01-10 03:29:11 | 2026-01-10 03:29:15 | 2026-01-10 03:29:17 | NULL  | {"sub_task_count":12,"read_local_sec":0,"read_local_mb":218,"read_remote_sec":0,"read_remote_mb":0,"read_segment_count":120,"write_segment_count":12,"write_segment_mb":218,"write_remote_sec":4,"in_queue_sec":23,"score_before":{"avg":10.0,"p50":10.0,"max":10.0},"score_after":{"avg":8.0,"p50":8.0,"max":8.0},"partial_success":false} |
+---------------------+-------+---------------------+---------------------+---------------------+-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

返回的字段包括：

- `Partition`：Compaction 任务所属的分区。
- `TxnID`：Compaction 任务的事务 ID。
- `StartTime`：Compaction 任务开始的时间。`NULL` 表示任务尚未启动。
- `CommitTime`：Compaction 任务提交数据的时间。`NULL` 表示数据尚未 Commit，如果 Commit，则必须 Publish 成功。
- `FinishTime`：Compaction 任务发布数据的时间。`NULL` 表示数据尚未 Publish。
- `Error`：Compaction 任务的错误信息（如有）。
- `Profile`：Compaction 任务 Commit 后的 Profile。
  - `sub_task_count`：分区中子任务（等同于 Tablet）的数量。
  - `read_local_sec`：所有子任务从本地缓存读取数据的总耗时。单位：秒。
  - `read_local_mb`：所有子任务从本地缓存读取数据的总大小。单位：MB。
  - `read_remote_sec`：所有子任务从远程存储读取数据的总耗时。单位：秒。
  - `read_remote_mb`：所有子任务从远程存储读取数据的总大小。单位：MB。
  - `read_segment_count`: 所有子任务读取的总文件数。
  - `write_segment_count`: 所有子任务新生成的总文件数。
  - `write_segment_mb`: 所有子任务新生成文件的总大小。单位：MB。
  - `write_remote_sec`: 所有子任务往远程存储写入数据的总耗时。单位：秒。
  - `in_queue_sec`：所有子任务排队的总时间。单位：秒。
  - `score_before`：Compaction 前分区的 Compaction Score。包含 `avg`、`p50` 和 `max` 字段。
  - `score_after`：Compaction 后分区的 Compaction Score。包含 `avg`、`p50` 和 `max` 字段。
  - `partial_success`：Compaction 任务是否部分成功（部分 Tablet 失败）。

#### 查看 Compaction 任务的执行详情

`information_schema.be_cloud_native_compactions` 中的每一行代表一个 Tablet Compaction 执行单元。启用
Tablet 并行 Compaction 后，每个并行 Subtask 分别占一行：它们具有相同的事务 ID 和 Tablet ID，但
`SUBTASK_ID` 不同。并行 Compaction 的 merged context 只是聚合对象，不是实际执行单元，因此不会返回。

示例：

```Plain
mysql> SELECT BE_ID, TXN_ID, TABLET_ID, VERSION, SKIPPED, RUNS, START_TIME, FINISH_TIME,
              PROGRESS, STATUS, PROFILE
       FROM information_schema.be_cloud_native_compactions;
+-------+--------+-----------+---------+---------+------+---------------------+-------------+----------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| BE_ID | TXN_ID | TABLET_ID | VERSION | SKIPPED | RUNS | START_TIME          | FINISH_TIME | PROGRESS | STATUS | PROFILE                                                                                                                                                                                         |
+-------+--------+-----------+---------+---------+------+---------------------+-------------+----------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 10001 |  51047 |     43034 |      12 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       82 |        | {"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51048 |     43032 |      12 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       82 |        | {"read_local_sec":0,"read_local_mb":32,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51049 |     43033 |      12 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       82 |        | {"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51051 |     43038 |       9 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       84 |        | {"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51052 |     43036 |      12 |       0 |    0 | NULL                | NULL        |        0 |        |                                                                                                                                                                                                 |
| 10001 |  51053 |     43035 |      12 |       0 |    1 | 2024-09-24 19:15:16 | NULL        |        2 |        | {"read_local_sec":0,"read_local_mb":1,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":100,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0}   |
+-------+--------+-----------+---------+---------+------+---------------------+-------------+----------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

返回的字段包括：

- `BE_ID`：CN 的 ID。
- `TXN_ID`：子任务所属事务的 ID。
- `TABLET_ID`：子任务所属 Tablet 的 ID。
- `VERSION`：Tablet 的版本。
- `RUNS`：子任务执行的次数。
- `START_TIME`：子任务开始的时间。
- `FINISH_TIME`：子任务完成的时间。
- `PROGRESS`：Tablet 的 Compaction 进度，以百分比表示。
- `STATUS`：子任务的状态。如果有错误，会在此字段中返回错误信息。
- `PROFILE`：（自 v3.2.12 和 v3.3.4 版本开始支持）子任务的实时 Profile。
  - `read_local_sec`：子任务从本地缓存读取数据的耗时。单位：秒。
  - `read_local_mb`：子任务从本地缓存读取的数据大小。单位：MB。
  - `read_remote_sec`：子任务从远程存储读取数据的耗时。单位：秒。
  - `read_remote_mb`：子任务从远程存储读取的数据大小。单位：MB。
  - `read_local_count`：子任务从本地缓存读取数据的次数。
  - `read_remote_count`：子任务从远程存储读取数据的次数。
  - `in_queue_sec`：子任务排队的时间。单位：秒。
- `SUBTASK_ID`：从 0 开始的并行 Subtask ID。普通非并行 Tablet Task 的值为 `NULL`。如果多行具有相同的
  `BE_ID`、`TXN_ID` 和 `TABLET_ID`，但 `SUBTASK_ID` 不同，它们就是同一个 Tablet Compaction 的并行执行单元。

`PROFILE` 中新增的全链路字段统一使用纳秒，便于精确对账：

- Profile 状态：
  - `profile_final`：任务执行中为 `false`，完成后为 `true`。实时 Profile 会将当前 attempt 已经过的时间计入
    `task_total_ns`；进入 executor 后，也会将当前 executor 已经过的时间计入 `task_execute_ns`。其他阶段计时在
    当前作用域退出时更新，因此实时 Profile 中的 `task_unaccounted_ns` 可能暂时偏大。
- Task 总体耗时：
  - `queue_wait_ns`：任务进入 CN Compaction 队列到 worker 开始执行的时间，不包含在 `task_total_ns` 中。
  - `task_prepare_ns`：选择输入 Rowset、加载 Tablet 元数据和构造 Compaction Task 的时间。
  - `task_execute_ns`：进入 Horizontal、Vertical 或 Index Compaction executor 后的 wall time；实时 Profile
    还包括当前 executor 已经过的时间。
  - `task_total_ns`：CN worker 开始准备任务到本次 Profile 快照或任务返回的总 wall time。
  - `task_accounted_ns`：所有互不重叠的顶层阶段之和。
  - `task_unaccounted_ns`：`task_total_ns - task_accounted_ns`。该值较大通常表示仍有阶段未打点，或存在调度、
    runtime 开销。
- 互不重叠的执行阶段：`input_prepare_ns`、`reader_prepare_ns`、`reader_open_ns`、`reader_get_next_ns`、
  `reader_close_ns`、`chunk_transform_ns`、`writer_create_ns`、`writer_open_ns`、`writer_write_ns`、
  `writer_flush_ns`、`writer_finish_ns`、`writer_close_ns`、`mask_io_ns`、`txn_log_build_ns`、
  `pk_sst_merge_ns`、`txn_log_write_ns`、`preload_compaction_state_ns` 和 `tablet_write_log_ns`。
- Reader 内部明细：`read_remote_ns`、`read_local_ns`、`create_segment_iter_ns`、`segment_init_ns`、
  `column_iterator_init_ns`、`block_load_ns`、`block_fetch_ns`、`block_seek_ns`、`decompress_ns`、
  `decode_dict_ns`，以及 Delete Vector/Filter 相关计时。这些字段包含在 Reader 顶层阶段内，不能再次累加到
  `task_accounted_ns`。
- Vertical Compaction 明细：`column_group_count`、`vertical_key_group_ns` 和 `vertical_value_group_ns`。
  Column Group 时间与 Reader/Writer 阶段重叠，仅用于诊断，不能重复累加。

Task 完成时，同一份单 Tablet Profile 还会连同 Tablet ID、Transaction ID、状态、Table ID 和 Partition ID
写入 CN INFO 日志。因此，即使该任务已经从 `be_cloud_native_compactions` 中消失，仍可以追溯单任务明细。

### 配置 Compaction 任务

您可以通过以下 FE 和 CN（BE）参数配置 Compaction 任务。

#### FE 参数

您可以动态配置以下 FE 参数。

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "-1");
```

##### lake_compaction_max_tasks

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下允许同时执行的 Compaction 任务数。系统依据分区中 Tablet 数量来计算 Compaction 任务数。如果一个分区有 10 个 Tablet，那么对该分区作一次 Compaction 就会创建 10 个 Compaction 子任务。如果正在执行中的 Compaction 任务数超过该阈值，系统将不会创建新的 Compaction 任务。将该值设置为 `0` 表示禁止 Compaction，设置为 `-1` 表示系统依据自适应策略自动计算该值，即存活的 CN 数量乘以 16。
- 引入版本：v3.1.0

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_disable_ids" = "11111;22222");
```

##### lake_compaction_disable_ids

- 默认值：""
- 类型：String
- 单位：-
- 是否动态：是
- 描述：禁止对指定表或分区发起 Compaction 任务，已发起的任务不会受到影响。此项的值为 Table ID 或 Partition ID，如有多个值，需使用分号(;)隔开。在 v3.4.5 和 v3.5.0 之前的版本中，该参数名为 `lake_compaction_disable_tables`，且仅支持 Table ID。旧参数名仍可作为别名使用。
- 引入版本：v3.2.7

#### CN 参数

您可以动态配置以下 CN 参数。

```SQL
UPDATE information_schema.be_configs SET VALUE = 8 
WHERE name = "compact_threads";
```

##### compact_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 引入版本：v3.0.0

> **说明**
>
> 在生产环境中，建议将 `compact_threads` 设置为 BE/CN CPU 核心数量的 25%。

##### max_cumulative_compaction_num_singleton_deltas

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Cumulative Compaction 能合并的最大 Segment 数。如果 Compaction 时出现内存不足的情况，可以调小该值。
- 引入版本：-

> **说明**
>
> 在生产环境中，建议将 `max_cumulative_compaction_num_singleton_deltas` 设置为 `100`，以加速 Compaction 任务并减少资源消耗。

##### lake_pk_compaction_max_input_rowsets

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，主键表 Compaction 任务中允许的最大输入 Rowset 数量。该参数默认值自 v3.2.4 和 v3.1.10 版本开始从 `5` 变更为 `1000`，并自 v3.3.1 和 v3.2.9 版本开始变更为 `500`。存算分离集群中的主键表在开启 Sized-tiered Compaction 策略后 (即设置 `enable_pk_size_tiered_compaction_strategy` 为 `true`)，无需通过限制每次 Compaction 的 Rowset 个数来降低写放大，因此调大该值。
- 引入版本：v3.1.8, v3.2.3

> **说明**
>
> 在生产环境中，建议将 `max_cumulative_compaction_num_singleton_deltas` 设置为 `100`，以加速 Compaction 任务并减少资源消耗。

##### lake_pk_compaction_max_rowsets_per_level

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，主键表的某个大小层中待合并的 Rowset 达到该数量时，即使该层得分不是最高，也会被合并。Size-tiered Compaction 策略每轮只合并得分最高的一层。持续导入时，最新写入的小 Rowset 所在层每轮得分都最高，由较大 Rowset（之前 Compaction 的产物和中等批次的导入）组成的层会一直增长，导致查询需要读取的 Segment 增多，并在 Compaction 刚完成时表现为 Compaction Score 的尖峰。当某个未被选中的层中待合并的 Rowset 达到该数量，且这些 Rowset 能与已选中的 Rowset 在同一次 Compaction 任务中合并时，该层会并入该次 Compaction 任务。设置为 `0` 表示关闭该行为。
- 引入版本：-

### 手动触发 Compaction 任务

以下语句异步提交手动请求。存算分离 PK Base Compaction 从 v4.1.4 开始支持；从 v4.1.7 开始，自动删除触发开关设为 `false` 也不影响手动请求。无删除行时会回退到普通 Compaction 选取。详情见[主键表 Base Compaction](#primary-key-base-compaction)。

```SQL
-- 触发整个表的 Compaction 任务。
ALTER TABLE <table_name> COMPACT;
-- 触发特定分区的 Compaction 任务。
ALTER TABLE <table_name> COMPACT <partition_name>;
-- 触发多个分区的 Compaction 任务。
ALTER TABLE <table_name> COMPACT (<partition_name>, <partition_name>, ...);
```

### 取消 Compaction 任务

您可以通过任务的事务 ID 手动取消 Compaction 任务。

```SQL
CANCEL COMPACTION WHERE TXN_ID = <TXN_ID>;
```

> **说明**
>
> - CANCEL COMPACTION 语句必须从 Leader FE 节点提交。
> - CANCEL COMPACTION 只能用于未 Commit 的事务，即 `SHOW PROC '/compactions'` 返回中 `CommitTime` 为 NULL 的事务。
> - CANCEL COMPACTION 为异步过程，您可以通过执行 `SHOW PROC '/compactions'` 查看任务是否取消。

## 最佳实践

由于 Compaction 对查询性能的影响非常重要，建议用户持续关注表和分区的后台数据合并情况。以下是一些最佳实践建议：

- 尽量调高导入的时间间隔（避免间隔 10 秒以内导入的场景），并且增加单次导入的批大小（避免 100 行数据以内的批大小）。
- 调整计算节点上的 Compaction 工作线程数，以加快任务执行速度。在生产环境中，建议将 `compact_threads` 的值设置为 BE/CN CPU 核心数量的 25%。
- 运用主要的两个命令 `show proc '/compactions'` 以及 `select * from information_schema.be_cloud_native_compactions;` 查看 Compaction 执行情况。
- 关注 Compaction Score，建议根据该指标配置告警。StarRocks 提供的 Grafana 监控模板已包含该指标。
- 监控 Compaction 的资源消耗情况，尤其是内存使用情况。Grafana 监控模板中也包含该项指标。

## 问题排查

### 慢查询

要排查存算分离集群中 Compaction 不及时导致的慢查询，可以在 SQL Profile 中查看单个 Fragment 内 `PhySegmentsCount` 除以 `TabletCount` 的值。如果是一个很大的值，比如几十以上，则可能是 Compaction 不及时导致查询慢。

`PhySegmentsCount` 仅在存算分离扫描中提供。对于存算一体集群，或不包含 `PhySegmentsCount` 的旧版本 Profile，改用单个 Fragment 内 `SegmentsReadCount` 除以 `TabletCount` 的值。在存算分离和存算一体扫描中，Tablet 内并行扫描都可能导致多个 split reader 在 `SegmentsReadCount` 中重复统计同一个 segment。因此，存在 `PhySegmentsCount` 时应优先使用它；在任一架构的并行扫描中使用回退计数器时，不能仅凭较高的比值判定存在 Compaction 问题。

### 集群最大 Compaction Score 很高

1. 通过 `ADMIN SHOW FRONTEND CONFIG LIKE "%lake_compaction%"` 以及 `SELECT * FROM information_schema.be_configs WHERE name = "compact_threads"` 检查 Compaction 相关的参数是否在合理区间。
2. 通过 `SHOW PROC '/compactions'` 查看 Compaction 是否卡住：
  - 如果 `CommitTime` 一直为 NULL，通过 `information_schema.be_cloud_native_compactions` 系统视图查看 Compaction 卡住原因；
  - 如果 `FinishTime` 一直为 NULL，通过 `TxnID` 在 FE 日志中查找 Publish 失败原因；
3. 通过 `SHOW PROC '/compactions'` 查看 Compaction 是否运行缓慢：
  - `sub_task_count` 太大（通过 `SHOW PARTITIONS` 查看这个分区每个 tablet 的大小），可能是表建的不合理；
  - `read_remote_mb` 太大（占据了整个 Compaction 读取数据量的30%以上），可以检查磁盘大小以及通过 `SHOW BACKENDS` 的 DataCacheMetrics 查看缓存 Quota；
  - `write_remote_sec` 太大（占据了整个 Compaction 的90%时间以上），可能是远端存储写入太慢，可以通过存算分离监控指标判定，关键词 `single upload latency` 和 `multi upload latency`；
  - `in_queue_sec` 太大（平均每个 Tablet 等待超过 60 秒以上），可能是参数设置不合理或者其他正在运行的 Compaction 太慢。
