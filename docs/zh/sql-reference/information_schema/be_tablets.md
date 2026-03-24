---
displayed_sidebar: docs
---

# be_tablets

`be_tablets` 提供有关每个 BE 节点上 Tablet 的信息。

`be_tablets` 提供以下字段：

| **字段**      | **描述**                                         |
| ------------- | ------------------------------------------------ |
| BE_ID         | BE 节点的 ID。                                   |
| TABLE_ID      | Tablet 所属表的 ID。                             |
| PARTITION_ID  | Tablet 所属分区的 ID。                           |
| TABLET_ID     | Tablet 的 ID。                                   |
| NUM_VERSION   | Tablet 中的版本数量。                            |
| MAX_VERSION   | Tablet 的最大版本。                              |
| MIN_VERSION   | Tablet 的最小版本。                              |
| NUM_ROWSET    | Tablet 中的 Rowset 数量。                        |
| NUM_ROW       | Tablet 中的行数。                                |
| DATA_SIZE     | Rowset **数据**的磁盘占用（Segment 列数据，字节）。不包括记录在 Rowset 元数据中的行内嵌索引（如 Short Key、Zone Map、Bloom Filter 等）。 |
| INDEX_MEM     | Tablet 的索引内存使用量（字节）。                |
| CREATE_TIME   | Tablet 的创建时间（Unix 时间戳，秒）。           |
| STATE         | Tablet 的状态（例如，`NORMAL`、`REPLICA_MISSING`）。 |
| TYPE          | Tablet 的类型。                                  |
| DATA_DIR      | Tablet 存储的数据目录。                          |
| SHARD_ID      | Tablet 的分片 ID。                               |
| SCHEMA_HASH   | Tablet 的 Schema Hash。                          |
| INDEX_DISK    | 对于**主键表** Tablet，表示**持久化主键索引**的磁盘占用（如云原生 PK 的 SSTable），不包括 Rowset 内嵌索引。其他类型 Tablet 上该字段可能为 0，即便 Rowset 存在磁盘索引数据。 |
| MEDIUM_TYPE   | Tablet 的介质类型（例如，`HDD`、`SSD`）。        |
| NUM_SEGMENT   | Tablet 中的 Segment 数量。                       |
