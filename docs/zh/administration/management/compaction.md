---
displayed_sidebar: docs
---

# 存算分离集群 Compaction

本文介绍如何在 StarRocks 存算分离集群中管理 Compaction。

## 概述

在 StarRocks 中，Compaction 是指将不同版本的数据文件合并为更大的文件，从而减少小文件的数量并提高查询效率。与存算一体集群相比，存算分离集群引入了一种新的 Compaction 调度机制，其特点是：

- Compaction 由 FE 调度并由 CN 执行。FE 以分区为单位发起 Compaction 任务。
- 每次 Compaction 都会生成一个新的数据版本，经历完整的数据导入事务流程，即数据写入、Commit 和 Publish。

## 管理 Compaction

### 查看 Compaction Score

系统为每个分区维护一个 Compaction Score。Compaction Score 反映了对应分区的数据文件合并状态。分数越高，表示该分区的数据文件合并程度越低，即该分区有更多版本的数据文件需要合并。FE 根据 Compaction Score 触发 Compaction 任务，您也可以通过 Compaction Score 判断特定分区的数据版本是否过多。

- 您可以通过 SHOW PROC 语句查看特定表中分区的 Compaction Score。

  ```Plain
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

- 从 v3.1.9 和 v3.2.4 版本开始，您也可以通过查询系统定义视图 `information_schema.partitions_meta` 查看分区的 Compaction Score。

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

您只需要关注以下两个指标：

- `AvgCS`：分区中所有 Tablet 的平均 Compaction Score。
- `MaxCS`：分区中所有 Tablet 的最大 Compaction Score。

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
+---------------------+-------+---------------------+---------------------+---------------------+-------+--------------------------------------------------------------------------------------------------------------------+
| Partition           | TxnID | StartTime           | CommitTime          | FinishTime          | Error | Profile                                                                                                            |
+---------------------+-------+---------------------+---------------------+---------------------+-------+--------------------------------------------------------------------------------------------------------------------+
| ssb.lineorder.43026 | 51053 | 2024-09-24 19:15:16 | NULL                | NULL                | NULL  | NULL                                                                                                               |
| ssb.lineorder.43027 | 51052 | 2024-09-24 19:15:16 | NULL                | NULL                | NULL  | NULL                                                                                                               |
| ssb.lineorder.43025 | 51047 | 2024-09-24 19:15:15 | NULL                | NULL                | NULL  | NULL                                                                                                               |
| ssb.lineorder.43026 | 51046 | 2024-09-24 19:15:04 | 2024-09-24 19:15:06 | 2024-09-24 19:15:06 | NULL  | {"sub_task_count":1,"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"in_queue_sec":0} |
| ssb.lineorder.43027 | 51045 | 2024-09-24 19:15:04 | 2024-09-24 19:15:06 | 2024-09-24 19:15:06 | NULL  | {"sub_task_count":1,"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"in_queue_sec":0} |
| ssb.lineorder.43029 | 51044 | 2024-09-24 19:15:03 | 2024-09-24 19:15:05 | 2024-09-24 19:15:05 | NULL  | {"sub_task_count":1,"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"in_queue_sec":0} |
+---------------------+-------+---------------------+---------------------+---------------------+-------+--------------------------------------------------------------------------------------------------------------------+
```

返回的字段包括：

- `Partition`：Compaction 任务所属的分区。
- `TxnID`：Compaction 任务的事务 ID。
- `StartTime`：Compaction 任务开始的时间。`NULL` 表示任务尚未启动。
- `CommitTime`：Compaction 任务提交数据的时间。`NULL` 表示数据尚未 Commit。
- `FinishTime`：Compaction 任务发布数据的时间。`NULL` 表示数据尚未 Publish。
- `Error`：Compaction 任务的错误信息（如有）。
- `Profile`：（自 v3.2.12 和 v3.3.4 版本开始支持）Compaction 任务完成后的 Profile。
  - `sub_task_count`：分区中子任务（等同于 Tablet）的数量。
  - `read_local_sec`：所有子任务从本地缓存读取数据的总耗时。单位：秒。
  - `read_local_mb`：所有子任务从本地缓存读取数据的总大小。单位：MB。
  - `read_remote_sec`：所有子任务从远程存储读取数据的总耗时。单位：秒。
  - `read_remote_mb`：所有子任务从远程存储读取数据的总大小。单位：MB。
  - `in_queue_sec`：所有子任务排队的总时间。单位：秒。

#### 查看 Compaction 任务的执行详情

每个 Compaction 任务被分解为多个子任务，每个子任务对应一个 Tablet。您可以通过查询系统定义视图 `information_schema.be_cloud_native_compactions` 查看每个子任务的执行详情。

示例：

```Plain
mysql> SELECT * FROM information_schema.be_cloud_native_compactions;
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

### 配置Compaction任务

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
ADMIN SET FRONTEND CONFIG ("lake_compaction_disable_tables" = "11111;22222");
```

##### lake_compaction_disable_tables

- 默认值：""
- 类型：String
- 单位：-
- 是否动态：是
- 描述：禁止对指定表发起 Compaction 任务，已发起的任务不会受到影响。此项的值为 Table ID，如有多个值，需使用分号(;)隔开。
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
> 在生产环境中，建议将 `max_cumulative_compaction_num_singleton_deltas` 设置为 `100`，以加速Compaction 任务并减少资源消耗。

##### lake_pk_compaction_max_input_rowsets

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，主键表 Compaction 任务中允许的最大输入 Rowset 数量。该参数默认值自 v3.2.4 和 v3.1.10 版本开始从 `5` 变更为 `1000`，并自 v3.3.1 和 v3.2.9 版本开始变更为 `500`。存算分离集群中的主键表在开启 Sized-tiered Compaction 策略后 (即设置 `enable_pk_size_tiered_compaction_strategy` 为 `true`)，无需通过限制每次 Compaction 的 Rowset 个数来降低写放大，因此调大该值。
- 引入版本：v3.1.8, v3.2.3

### 手动触发 Compaction 任务

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

- 关注 Compaction Score，建议根据该指标配置告警。StarRocks 提供的 Grafana 监控模板已包含该指标。
- 监控 Compaction 的资源消耗情况，尤其是内存使用情况。Grafana 监控模板中也包含该项指标。
- 调整计算节点上的 Compaction 并行工作线程数，以加快任务执行速度。建议将 `compact_threads` 的值设置为 BE/CN CPU 核心数量的 25%。在集群较空闲时，例如只需执行 Compaction 而无需处理查询时，可以暂时调整为 CPU 数量的 50%，在任务完成后再调回至 25%。

