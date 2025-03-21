---
displayed_sidebar: docs
---

# Iceberg 元数据表

本文描述了如何查看 StarRocks 中 Iceberg 表的元数据信息。

## 概述

从 v3.4.1 开始，StarRocks 支持 Iceberg 元数据表。这些元数据表包含了有关 Iceberg 表的各种信息，如表变更历史、快照和 Manifest。您可以通过将元数据表的名称附加到原始表名后来查询每个元数据表。

目前，StarRocks 支持以下 Iceberg 元数据表：

| **元数据表**           | **描述**                                                     |
| :--------------------- | :----------------------------------------------------------- |
| `history`              | 显示对表进行的元数据更改日志。                               |
| `metadata_log_entries` | 显示表的元数据日志条目。                                     |
| `snapshots`            | 显示表快照的详细信息。                                       |
| `manifests`            | 显示与表日志中快照相关联的 Manifest 概览。                   |
| `partitions`           | 显示表中分区的详细信息。                                     |
| `files`                | 显示当前快照中数据文件（Data File）和删除文件（Delete File）的详细信息。 |
| `refs`                 | 显示关于 Iceberg 引用（Reference）的详细信息，包括分支和标签。 |

## `history` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$history;
```

输出：

| **字段**            | **描述**                     |
| :------------------ | :--------------------------- |
| made_current_at     | 此快照成为当前快照的时间。   |
| snapshot_id         | 此快照的 ID。                |
| parent_id           | 父快照的 ID。                |
| is_current_ancestor | 此快照是否为当前快照的祖先。 |

## `metadata_log_entries` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$metadata_log_entries;
```

输出：

| **字段**               | **描述**                        |
| :--------------------- | :------------------------------ |
| timestamp              | 元数据被记录的时间。            |
| file                   | 元数据文件的位置。              |
| latest_snapshot_id     | 元数据更新时最新快照的 ID。     |
| latest_schema_id       | 元数据更新时最新 Schema 的 ID。 |
| latest_sequence_number | 元数据文件的数据序列号。        |

## `snapshots` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$snapshots;
```

输出：

| **字段**      | **描述**                                                     |
| :------------ | :----------------------------------------------------------- |
| committed_at  | 此快照提交的时间。                                           |
| snapshot_id   | 此快照的 ID。                                                |
| parent_id     | 父快照的 ID。                                                |
| operation     | 对 Iceberg 表执行的操作类型。有效值：<ul><li>`append`：新增数据。</li><li>`replace`：删除并替换文件，数据保持不变。</li><li>`overwrite`：新数据覆盖写旧数据。</li><li>`delete`：删除表中的数据。</li></ul> |
| manifest_list | 包含快照更改详细信息的 Avro Manifest 文件列表。              |
| summary       | 从上一个快照到当前快照的变更摘要。                           |

## `manifests` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$manifests;
```

输出：

| **字段**                  | **描述**                                                |
| :------------------------ | :------------------------------------------------------ |
| path                      | Manifest 文件的位置。                                   |
| length                    | Manifest 文件的长度。                                   |
| partition_spec_id         | 用于写入 Manifest 文件的分区规范 ID。                   |
| added_snapshot_id         | 添加此 Manifest 条目的快照 ID。                         |
| added_data_files_count    | Manifest 文件中状态为 `ADDED` 的数据文件数量。          |
| added_rows_count          | Manifest 文件中所有状态为 `ADDED` 数据文件的总行数。    |
| existing_data_files_count | Manifest 文件中状态为 `EXISTING` 的数据文件数量。       |
| existing_rows_count       | Manifest 文件中所有状态为 `EXISTING` 数据文件的总行数。 |
| deleted_data_files_count  | Manifest 文件中状态为 `DELETED` 的数据文件数量。        |
| deleted_rows_count        | Manifest 文件中所有状态为 `DELETED` 数据文件的总行数。  |
| partition_summaries       | 分区范围元数据。                                        |

## `partitions` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$partitions;
```

输出：

| **字段**                      | **描述**                              |
| :---------------------------- | :------------------------------------ |
| partition_value               | 分区列名称与分区列值的映射。          |
| spec_id                       | 文件的分区规范 ID。                   |
| record_count                  | 分区中的记录数。                      |
| file_count                    | 分区中映射的文件数。                  |
| total_data_file_size_in_bytes | 分区中所有数据文件的大小。            |
| position_delete_record_count  | 分区中 Position Delete 文件的总行数。 |
| position_delete_file_count    | 分区中 Position Delete 文件数量。     |
| equality_delete_record_count  | 分区中 Equality Delete 文件的总行数。 |
| equality_delete_file_count    | 分区中 Equality Delete 文件数量。     |
| last_updated_at               | 分区最近更新时间。                    |

## `files` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$files;
```

输出：

| **字段**           | **描述**                                                     |
| :----------------- | :----------------------------------------------------------- |
| content            | 文件中存储的内容类型。有效值：`DATA(0)`、`POSITION_DELETES(1)`、`EQUALITY_DELETES(2)`。 |
| file_path          | 数据文件的位置。                                             |
| file_format        | 数据文件的格式。                                             |
| spec_id            | 用于跟踪包含行的文件的 Spec ID。                             |
| record_count       | 数据文件中包含的条目数。                                     |
| file_size_in_bytes | 数据文件的大小。                                             |
| column_sizes       | Iceberg 列 ID 与文件中对应大小的映射。                       |
| value_counts       | Iceberg 列 ID 与文件中对应条目数的映射。                     |
| null_value_counts  | Iceberg 列 ID 与文件中 `NULL` 值数量的映射。                 |
| nan_value_counts   | Iceberg 列 ID 与文件中非数值类型值数量的映射。               |
| lower_bounds       | Iceberg 列 ID 与文件中对应的下界的映射。                     |
| upper_bounds       | Iceberg 列 ID 与文件中对应的上界的映射。                     |
| split_offsets      | 推荐的拆分位置列表。                                         |
| sort_id            | 代表该文件排序顺序的 ID。                                    |
| equality_ids       | 用于在相等删除文件（Equality Delete File）中进行相等比较的字段 ID 集。 |
| key_metadata       | 文件加密所使用的密钥元数据（如适用）。                       |

## `refs` 表

用法：

```SQL
SELECT * FROM [<catalog>.][<database>.]table$refs;
```

输出：

| **字段**                | **描述**                                                     |
| :---------------------- | :----------------------------------------------------------- |
| name                    | 引用的名称。                                                 |
| type                    | 引用的类型。有效值：`BRANCH` 或 `TAG`。                      |
| snapshot_id             | 引用的快照 ID。                                              |
| max_reference_age_in_ms | 引用的最长保留时间，超出此时间引用可能会过期。               |
| min_snapshots_to_keep   | 仅适用于分支，分支中必须保留的最小快照数。                   |
| max_snapshot_age_in_ms  | 仅适用于分支，分支中允许的最长保留时间。过期的快照将被删除。 |
