---
displayed_sidebar: "Chinese"
---

# loads

提供导入作业的结果信息。此视图自 StarRocks v3.1 版本起支持。当前仅支持查看 [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 和 [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md) 导入作业的结果信息。

`loads` 提供以下字段：

| **字段**             | **说明**                                                     |
| -------------------- | ------------------------------------------------------------ |
| JOB_ID               | 导入作业的 ID，由 StarRocks 自动生成。                       |
| LABEL                | 导入作业的标签。                                             |
| DATABASE_NAME        | 目标 StarRocks 表所在的数据库的名称。                        |
| STATE                | 导入作业的状态，包括：<ul><li>`PENDING`：导入作业已创建。</li><li>`QUEUEING`：导入作业正在等待执行中。</li><li>`LOADING`：导入作业正在执行中。</li><li>`PREPARED`：事务已提交。</li><li>`FINISHED`：导入作业成功。</li><li>`CANCELLED`：导入作业失败。</li></ul>请参见[异步导入](../../loading/Loading_intro.md#异步导入)。 |
| PROGRESS             | 导入作业 ETL 阶段和 LOADING 阶段的进度。                     |
| TYPE                 | 导入作业的类型。如果是 Broker Load 导入，则返回 `BROKER`。如果是 INSERT 导入，则返回 `INSERT`。 |
| PRIORITY             | 导入作业的优先级。取值范围：`HIGHEST`、`HIGH`、`NORMAL`、`LOW`、`LOWEST`。 |
| SCAN_ROWS            | 扫描的数据行总数。                                           |
| FILTERED_ROWS        | 因数据质量不合格而过滤掉的错误数据行总数。                   |
| UNSELECTED_ROWS      | 根据 WHERE 子句中指定的条件过滤掉的数据行总数。              |
| SINK_ROWS            | 完成导入的数据行总数。                                       |
| ETL_INFO             | ETL 详情。该字段只对 Spark Load 作业有效。如果是其他类型的导入作业，则返回结果为空。 |
| TASK_INFO            | 导入作业的详情，比如导入作业的超时时长 (`timeout`) 和最大容忍率 (`max_filter_ratio`) 等。 |
| CREATE_TIME          | 导入作业的创建时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| ETL_START_TIME       | ETL 阶段的开始时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| ETL_FINISH_TIME      | ETL 阶段的结束时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | LOADING 阶段的开始时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | LOADING 阶段的结束时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| JOB_DETAILS          | 导入数据的详情，包括导入的数据大小（按字节数计）、文件个数等。 |
| ERROR_MSG            | 导入作业的错误信息。如果没有错误，则返回 `NULL`。            |
| TRACKING_URL         | 导入作业中质量不合格数据采样的访问地址。可以使用 `curl` 命令或 `wget` 命令访问该地址。如果导入作业中不存在质量不合格的数据，则返回 `NULL`。 |
| TRACKING_SQL         | 导入作业跟踪日志的查询语句。仅在导入作业包含不合格数据时才会返回查询语句。如果导入作业未包含不合格数据，则返回 `NULL`。 |
| REJECTED_RECORD_PATH | 导入作业中质量不合格数据的访问地址。返回的不合格数据结果条数，由导入作业中 `log_rejected_record_num` 参数的设置决定。您可以使用 `wget` 命令访问该地址。如果导入作业中不存在质量不合格的数据，则返回 `NULL`。 |
