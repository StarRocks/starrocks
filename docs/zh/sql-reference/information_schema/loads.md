---
displayed_sidebar: docs
---

# loads

提供导入作业的结果信息。此视图自 StarRocks v3.1 版本起支持。当前仅支持查看 [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 和 [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 导入作业的结果信息。

`loads` 提供以下字段：

| **字段**             | **说明**                                                     |
| -------------------- | ------------------------------------------------------------ |
| ID                   | 导入作业的唯一 ID，由 StarRocks 自动生成。                   |
| LABEL                | 导入作业的标签。                                             |
| PROFILE_ID           | 导入作业的 Profile ID。                                      |
| DB_NAME              | 目标 StarRocks 表所在的数据库的名称。                        |
| TABLE_NAME           | 导入数据到的表的名称。                                       |
| USER                 | 提交导入作业的用户。                                         |
| WAREHOUSE            | 执行导入作业的 Warehouse。                                   |
| STATE                | 导入作业的状态，包括：`PENDING`、`QUEUEING`、`LOADING`、`PREPARED`、`FINISHED`、`CANCELLED`。 |
| PROGRESS             | 导入作业 ETL 阶段和 LOADING 段的进度。                     |
| TYPE                 | 导入作业的类型。如果是 Broker Load 导入，则返回 `BROKER`。如果是 INSERT 导入，则返回 `INSERT`。 |
| PRIORITY             | 导入作业的优先级。取值范围：`HIGHEST`、`HIGH`、`NORMAL`、`LOW`、`LOWEST`。 |
| SCAN_ROWS            | 扫描的数据行总数。                                           |
| SCAN_BYTES           | 扫描的数据字节数。                                           |
| FILTERED_ROWS        | 因数据质量不合格而过滤掉的错误数据行总数。                   |
| UNSELECTED_ROWS      | 根据 WHERE 子句中指定的条件过滤掉的数据行总数。              |
| SINK_ROWS            | 完成导入的数据行总数。                                       |
| RUNTIME_DETAILS      | 导入作业的运行时详情，JSON 格式。                            |
| CREATE_TIME          | 导入作业的创建时间。                                         |
| LOAD_START_TIME      | 导入作业的开始时间。                                         |
| LOAD_COMMIT_TIME     | 导入作业的提交时间。                                         |
| LOAD_FINISH_TIME     | 导入作业的完成时间。                                         |
| PROPERTIES           | 导入作业的属性，JSON 格式。                                  |
| ERROR_MSG            | 导入作业的错误信息。如果导入作业未遇到任何错误，则返回 `NULL`。 |
| TRACKING_SQL         | 用于跟踪导入作业的 SQL 语句。仅在导入作业包含不合格数据时才会返回查询语句。如果导入作业未包含不合格数据，则返回 `NULL`。 |
| REJECTED_RECORD_PATH | 拒绝记录文件的路径。                                         |
| JOB_ID               | 导入作业的 ID。                                              |
