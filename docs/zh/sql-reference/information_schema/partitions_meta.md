---
displayed_sidebar: docs
---

# partitions_meta

`partitions_meta` 提供有关表分区的信息。

`partitions_meta` 提供以下字段：

| **字段**                      | **描述**                                         |
| ----------------------------- | ------------------------------------------------ |
| DB_NAME                       | 分区所属数据库的名称。                           |
| TABLE_NAME                    | 分区所属表的名称。                               |
| PARTITION_NAME                | 分区的名称。                                     |
| PARTITION_ID                  | 分区的 ID。                                      |
| COMPACT_VERSION               | 分区的 Compact 版本。                            |
| VISIBLE_VERSION               | 分区的可见版本。                                 |
| VISIBLE_VERSION_TIME          | 分区的可见版本时间。                             |
| NEXT_VERSION                  | 分区的下一个版本。                               |
| DATA_VERSION                  | 分区的数据版本。                                 |
| VERSION_EPOCH                 | 分区的版本 Epoch。                               |
| VERSION_TXN_TYPE              | 分区的版本事务类型。                             |
| PARTITION_KEY                 | 分区的分区键。                                   |
| PARTITION_VALUE               | 分区的值（例如，`Range` 或 `List`）。            |
| DISTRIBUTION_KEY              | 分区的分布键。                                   |
| BUCKETS                       | 分区中的 Bucket 数量。                           |
| REPLICATION_NUM               | 分区的副本数量。                                 |
| STORAGE_MEDIUM                | 分区的存储介质。                                 |
| COOLDOWN_TIME                 | 分区的冷却时间。                                 |
| LAST_CONSISTENCY_CHECK_TIME   | 分区上次一致性检查时间。                         |
| IS_IN_MEMORY                  | 指示分区是否在内存中（`true`）或不在（`false`）。 |
| IS_TEMP                       | 指示分区是否为临时分区（`true`）或不是（`false`）。 |
| DATA_SIZE                     | 分区的数据大小。                                 |
| ROW_COUNT                     | 分区中的行数。                                   |
| ENABLE_DATACACHE              | 指示分区是否启用数据缓存（`true`）或不启用（`false`）。 |
| AVG_CS                        | 分区的平均 Compaction Score。                    |
| P50_CS                        | 分区的 50 百分位 Compaction Score。              |
| MAX_CS                        | 分区的最大 Compaction Score。                    |
| STORAGE_PATH                  | 分区的存储路径。                                 |
| STORAGE_SIZE                  | 分区的存储大小。                                 |
| METADATA_SWITCH_VERSION       | 分区的元数据切换版本。                           |
| TABLET_BALANCED               | 分区的 Tablet 分布是否均衡。                     |
