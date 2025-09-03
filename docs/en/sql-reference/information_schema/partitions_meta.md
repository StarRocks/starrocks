---
displayed_sidebar: docs
---

# partitions_meta

`partitions_meta` provides information about partitions of tables.

The following fields are provided in `partitions_meta`:

| **Field**                     | **Description**                                              |
| ----------------------------- | ------------------------------------------------------------ |
| DB_NAME                       | Name of the database to which the partition belongs.         |
| TABLE_NAME                    | Name of the table to which the partition belongs.            |
| PARTITION_NAME                | Name of the partition.                                       |
| PARTITION_ID                  | ID of the partition.                                         |
| COMPACT_VERSION               | Compact version of the partition.                            |
| VISIBLE_VERSION               | Visible version of the partition.                            |
| VISIBLE_VERSION_TIME          | Visible version time of the partition.                       |
| NEXT_VERSION                  | Next version of the partition.                               |
| DATA_VERSION                  | Data version of the partition.                               |
| VERSION_EPOCH                 | Version epoch of the partition.                              |
| VERSION_TXN_TYPE              | Version transaction type of the partition.                   |
| PARTITION_KEY                 | Partition key of the partition.                              |
| PARTITION_VALUE               | Partition value of the partition (e.g., `Range` or `List`).  |
| DISTRIBUTION_KEY              | Distribution key of the partition.                           |
| BUCKETS                       | Number of buckets in the partition.                          |
| REPLICATION_NUM               | Replication number of the partition.                         |
| STORAGE_MEDIUM                | Storage medium of the partition.                             |
| COOLDOWN_TIME                 | Cooldown time of the partition.                              |
| LAST_CONSISTENCY_CHECK_TIME   | Last consistency check time of the partition.                |
| IS_IN_MEMORY                  | Indicates whether the partition is in memory (`true`) or not (`false`). |
| IS_TEMP                       | Indicates whether the partition is temporary (`true`) or not (`false`). |
| DATA_SIZE                     | Data size of the partition.                                  |
| ROW_COUNT                     | Number of rows in the partition.                             |
| ENABLE_DATACACHE              | Indicates whether data cache is enabled for the partition (`true`) or not (`false`). |
| AVG_CS                        | Average compaction score of the partition.                   |
| P50_CS                        | 50th percentile compaction score of the partition.           |
| MAX_CS                        | Maximum compaction score of the partition.                   |
| STORAGE_PATH                  | Storage path of the partition.                               |
| STORAGE_SIZE                  | Storage size of the partition.                               |
| METADATA_SWITCH_VERSION       | Metadata switch version of the partition.                    |
| TABLET_BALANCED               | Whether the tablet distribution is balanced in the partition. |
