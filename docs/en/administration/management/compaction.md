---
displayed_sidebar: docs
---

# Compaction for Shared-data Clusters

This topic describes how to manage compaction in StarRocks shared-data clusters.

## Overview

In StarRocks, compaction refers to the process of merging different versions of data files into larger files, thereby reducing the number of small files and improving query efficiency. Compared to shared-nothing clusters, shared-data clusters introduce a new compaction scheduling mechanism, characterized by:

- Compaction is scheduled by the FE and executed by CN. FE initiates compaction tasks on a partition basis.
- Each compaction generates a new version of data by following the complete transactional process of data loading: write, commit, and publish.

## Manage compaction

### View compaction scores

The system maintains a compaction score for each partition. A compaction score reflects the data file merging status of the corresponding partition. The higher the score, the lower the merging level of the partition, that is, the more versions of data files pending to be merged. FE uses compaction scores as the reference for triggering compaction tasks, while you can use compaction scores as an indicator of whether there are too many data versions in the partition.

- You can view the compaction scores of partitions in a specific table by using the SHOW PROC statement.

  ```Plain
  SHOW PROC '/dbs/<database_name>/<table_name>/partitions'
  ```

  Example:

  ```Plain
  mysql> SHOW PROC '/dbs/load_benchmark/store_sales/partitions';
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  | PartitionId | PartitionName | CompactVersion | VisibleVersion | NextVersion | State  | PartitionKey | Range | DistributionKey              | Buckets | DataSize | RowCount  | CacheTTL | AsyncWrite | AvgCS | P50CS | MaxCS |
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  | 38028       | store_sales   | 913            | 921            | 923         | NORMAL |              |       | ss_item_sk, ss_ticket_number | 64      | 15.6GB   | 273857126 | 2592000  | false      | 10.00 | 10.00 | 10.00 |
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  1 row in set (0.20 sec)
  ```

- From v3.1.9 and v3.2.4 onwards, you can also view the partition compaction scores by querying the system-defined view `information_schema.partitions_meta`.

  Example:

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

You only need to focus on the following two metrics:

- `AvgCS`: The average compaction score of all tablets in the partition.
- `MaxCS`: The maximum compaction score among all tablets in the partition.

### View compaction tasks

As new data is loading to the system, FE constantly schedules compaction tasks to be executed on different CN nodes. You can first view the general status of compaction tasks on FE, and then view the execution details of each tasks on CN.

#### View general status of compaction tasks

You can view the general status of compaction tasks using the SHOW PROC statement.

```SQL
SHOW PROC '/compactions';
```

Example:

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

The following fields are returned:

- `Partition`: The partition to which the compaction task belongs.
- `TxnID`: The transaction ID assigned to the compaction task.
- `StartTime`: The time when the compaction task starts. `NULL` indicates that the task has not yet been initiated.
- `CommitTime`: The time when the compaction task commits the data. `NULL` indicates that the data has not yet been committed.
- `FinishTime`: The time when the compaction task publishes the data. `NULL` indicates that the data has not yet been published.
- `Error`: The error message (if any) of the compaction task.
- `Profile`: (supported from v3.2.12 and v3.3.4) The Profile of the compaction task after finished.
  - `sub_task_count`: The number of sub-tasks (equivalent to tablets) in the partition.
  - `read_local_sec`: The total time consumption of all sub-tasks on reading data from the local cache. Unit: Seconds.
  - `read_local_mb`: The total size of data read from the local cache by all sub-tasks. Unit: MB.
  - `read_remote_sec`: The total time consumption of all sub-tasks on reading data from the remote storage. Unit: Seconds.
  - `read_remote_mb`The total size of data read from the remote storage by all sub-tasks. Unit: MB.
  - `in_queue_sec`: The total time of all sub-tasks staying in the queue. Unit: Seconds.

#### View execution details of compaction tasks

Each compaction task is divided into multiple sub-tasks, each of which corresponds to a tablet. You can view the execution details of each sub-task by querying the system-defined view `information_schema.be_cloud_native_compactions`.

Example:

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

The following fields are returned:

- `BE_ID`: The ID of the CN.
- `TXN_ID`: The ID of transaction to which the sub-task belongs.
- `TABLET_ID`: The ID of tablet to which the sub-task belongs.
- `VERSION`: The version of the tablet.
- `RUNS`: The number of times the sub-task has been executed.
- `START_TIME`: The time when the sub-task starts.
- `FINISH_TIME`: The time when the sub-task finishes.
- `PROGRESS`: The compaction progress of the tablet in percentage.
- `STATUS`: The status of the sub-task. Error messages will be returned in this field if there is an error.
- `PROFILE`: (supported from v3.2.12 and v3.3.4) The runtime profile of the sub-task.
  - `read_local_sec`: The time consumption of the sub-task on reading data from the local cache. Unit: Seconds.
  - `read_local_mb`: The size of data read from the local cache by the sub-task. Unit: MB.
  - `read_remote_sec`: The time consumption of the sub-task on reading data from the remote storage. Unit: Seconds.
  - `read_remote_mb`The size of data read from the remote storage by the sub-task. Unit: MB.
  - `read_local_count`: The number of times the sub-task reads data from the local cache.
  - `read_remote_count`: The number of times the sub-task reads data from the remote storage.
  - `in_queue_sec`: The time of the sub-task staying in queue. Unit: Seconds.

### Configure compaction tasks

You can configure compaction tasks using these FE and CN (BE) parameters.

#### FE parameter

You can configure the following FE parameter dynamically.

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "-1");
```

##### lake_compaction_max_tasks

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Compaction tasks allowed in a shared-data cluster. Setting this item to `-1` indicates calculating the concurrent task number in an adaptive manner, that is, the number of surviving CN nodes multiplied by 16. Setting this value to `0` will disable compaction.
- Introduced in: v3.1.0

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_disable_tables" = "11111;22222");
```

##### lake_compaction_disable_tables

- Default：""
- Type：String
- Unit：-
- Is mutable：Yes
- Description：Disable compaction for certain tables. This will not affect compaction that has started. The value of this item is table ID. Multiple values are separated by ';'.
- Introduced in：v3.2.7

#### CN parameters

You can configure the following CN parameter dynamically.

```SQL
UPDATE information_schema.be_configs SET VALUE = 8 
WHERE name = "compact_threads";
```

##### compact_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for concurrent compaction tasks. This configuration is changed to dynamic from v3.1.7 and v3.2.2 onwards.
- Introduced in: v3.0.0

> **NOTE**
>
> In production, it is recommended to set `compact_threads` to 25% of the BE/CN CPU core count.

##### max_cumulative_compaction_num_singleton_deltas

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be merged in a single Cumulative Compaction. You can reduce this value if OOM occurs during compaction.
- Introduced in: -

> **NOTE**
>
> In production, it is recommended to set `max_cumulative_compaction_num_singleton_deltas` to `100` to accelerate the compaction tasks and reduce their recource consumption.

##### lake_pk_compaction_max_input_rowsets

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of input rowsets allowed in a Primary Key table compaction task in a shared-data cluster. The default value of this parameter is changed from `5` to `1000` since v3.2.4 and v3.1.10, and to `500` since since v3.3.1 and v3.2.9. After the Sized-tiered Compaction policy is enabled for Primary Key tables (by setting `enable_pk_size_tiered_compaction_strategy` to `true`), StarRocks does not need to limit the number of rowsets for each compaction to reduce write amplification. Therefore, the default value of this parameter is increased.
- Introduced in: v3.1.8, v3.2.3

### Manually trigger compaction tasks

```SQL
-- Trigger compaction for the whole table.
ALTER TABLE <table_name> COMPACT;
-- Trigger compaction for a specific partition.
ALTER TABLE <table_name> COMPACT <partition_name>;
-- Trigger compaction for multiple partitions.
ALTER TABLE <table_name> COMPACT (<partition_name>, <partition_name>, ...);
```

### Cancel compaction tasks

You can manually cancel a compaction task using the transaction ID of the task.

```SQL
CANCEL COMPACTION WHERE TXN_ID = <TXN_ID>;
```

> **NOTE**
>
> - The CANCEL COMPACTION statement must be submitted from the Leader FE node.
> - The CANCEL COMPACTION statement only applies to transactions that have not committed, that is, `CommitTime` is NULL in the return of `SHOW PROC '/compactions'`.
> - CANCEL COMPACTION is an asynchronous process. You can check if the task is cancelled by executing `SHOW PROC '/compactions'`.

## Best practices

Since Compaction is crucial for query performance, it is recommended to regularly monitor the data merging status of tables and partitions. Here are some best practices and guidelines:

- Monitor the compaction score, and configure alerts based on it. StarRocks' built-in Grafana monitoring template includes this metric.
- Pay attention to the resource consumption during compaction, especially memory usage. The Grafana monitoring template also includes this metric.
- Adjust the number of parallel compaction worker threads on CN to accelerate task execution. It is recommended to set `compact_threads` to 25% of the BE/CN CPU core count. When the cluster is idle (for example, only performing compaction and not handling queries), you can temporarily increase this value to 50%, and revert to 25% after the task is complete.
