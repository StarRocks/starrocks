---
displayed_sidebar: docs
---

# be_txns

`be_txns` 提供有关每个 BE 节点上事务的信息。

`be_txns` 提供以下字段：

| **字段**       | **描述**                                         |
| -------------- | ------------------------------------------------ |
| BE_ID          | BE 节点的 ID。                                   |
| LOAD_ID        | 加载任务的 ID。                                  |
| TXN_ID         | 事务的 ID。                                      |
| PARTITION_ID   | 事务涉及的分区 ID。                              |
| TABLET_ID      | 事务涉及的 Tablet ID。                           |
| CREATE_TIME    | 事务创建时间（Unix 时间戳，秒）。                |
| COMMIT_TIME    | 事务提交时间（Unix 时间戳，秒）。                |
| PUBLISH_TIME   | 事务发布时间（Unix 时间戳，秒）。                |
| ROWSET_ID      | 事务涉及的 Rowset ID。                           |
| NUM_SEGMENT    | Rowset 中的 Segment 数量。                       |
| NUM_DELFILE    | Rowset 中的删除文件数量。                        |
| NUM_ROW        | Rowset 中的行数。                                |
| DATA_SIZE      | Rowset 的数据大小（字节）。                      |
| VERSION        | Rowset 的版本。                                  |
