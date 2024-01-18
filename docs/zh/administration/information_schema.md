---
displayed_sidebar: "Chinese"
---

# Information Schema

`information_schema` 是 StarRocks 实例中的一个数据库。该数据库包含数张由系统定义的表，这些表中存储了关于 StarRocks 实例中所有对象的大量元数据信息。

自 v3.2 起，StarRocks 支持通过 `information_schema` 查看 External Catalog 元数据。

## 通过 Information Schema 查看元数据信息

您可以通过查询 `information_schema` 中的表来查看 StarRocks 实例中的元数据信息。

以下示例通过查询表 `tables` 查看 StarRocks 中名为 `sr_member` 的表相关的元数据信息。

```Plain
mysql> SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'sr_member'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: sr_hub
     TABLE_NAME: sr_member
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: NULL
     TABLE_ROWS: 6
 AVG_ROW_LENGTH: 542
    DATA_LENGTH: 3255
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2022-11-17 14:32:30
    UPDATE_TIME: 2022-11-17 14:32:55
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: NULL
  TABLE_COMMENT: OLAP
1 row in set (1.04 sec)
```

## Information Schema 表

StarRocks 优化了 `information_schema` 中表 `tables`、表 `tables_config` 和表 `load_tracking_logs` 提供的元数据信息，并提供了表 `loads`（从 3.1 版本起）：

| **Information Schema 表名** | **描述**                                  |
| --------------------------- | ---------------------------------------- |
| [tables](#tables)                      | 提供常规的表元数据信息。                     |
| [tables_config](#tables_config)               | 提供额外的 StarRocks 独有的表元数据信息。     |
| [load_tracking_logs](#load_tracking_logs)          | 提供导入作业相关的错误信息。                  |
| [loads](#loads)                       | 提供导入作业的结果信息，从 3.1 版本起支持。当前仅支持查看 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 和 [Insert](../sql-reference/sql-statements/data-manipulation/INSERT.md) 导入作业的结果信息。                  |

### loads

表 `loads` 提供以下字段：

| **字段**             | **说明**                                                     |
| -------------------- | ------------------------------------------------------------ |
| JOB_ID               | 导入作业的 ID，由 StarRocks 自动生成。                       |
| LABEL                | 导入作业的标签。                                             |
| DATABASE_NAME        | 目标 StarRocks 表所在的数据库的名称。                        |
| STATE                | 导入作业的状态，包括：<ul><li>`PENDING`：导入作业已创建。</li><li>`QUEUEING`：导入作业正在等待执行中。</li><li>`LOADING`：导入作业正在执行中。</li><li>`PREPARED`：事务已提交。</li><li>`FINISHED`：导入作业成功。</li><li>`CANCELLED`：导入作业失败。</li></ul>请参见[异步导入](../loading/Loading_intro.md#异步导入)。 |
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

### tables

表 `tables` 提供以下字段：

| **字段**        | **描述**                                                     |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | 表所属的 Catalog 名称。                                      |
| TABLE_SCHEMA    | 表所属的数据库名称。                                         |
| TABLE_NAME      | 表名。                                                       |
| TABLE_TYPE      | 表的类型。 有效值：“BASE TABLE” 或 “VIEW”。                  |
| ENGINE          | 表的引擎类型。 有效值：“StarRocks”、“MySQL”、“MEMORY”或空字符串。 |
| VERSION         | 该字段暂不可用。                                             |
| ROW_FORMAT      | 该字段暂不可用。                                             |
| TABLE_ROWS      | 表的行数。                                                   |
| AVG_ROW_LENGTH  | 表的平均行长度（大小），等于 `DATA_LENGTH` / `TABLE_ROWS`。 单位：Byte。 |
| DATA_LENGTH     | 表的数据文件长度（大小）。单位：Byte。                         |
| MAX_DATA_LENGTH | 该字段暂不可用。                                             |
| INDEX_LENGTH    | 该字段暂不可用。                                             |
| DATA_FREE       | 该字段暂不可用。                                             |
| AUTO_INCREMENT  | 该字段暂不可用。                                             |
| CREATE_TIME     | 创建表的时间。                                               |
| UPDATE_TIME     | 最后一次更新表的时间。                                       |
| CHECK_TIME      | 最后一次对表进行一致性检查的时间。                           |
| TABLE_COLLATION | 表的默认 Collation。                                         |
| CHECKSUM        | 该字段暂不可用。                                             |
| CREATE_OPTIONS  | 该字段暂不可用。                                             |
| TABLE_COMMENT   | 表的 Comment。                                               |

### tables_config

表 `tables_config` 提供以下字段：

| **字段**         | **描述**                                                     |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | 表所属的数据库名称。                                         |
| TABLE_NAME       | 表名。                                                       |
| TABLE_ENGINE     | 表的引擎类型。                                               |
| TABLE_MODEL      | 表类型。有效值：“DUP_KEYS”、“AGG_KEYS”、“UNQ_KEYS” 或 “PRI_KEYS”。 |
| PRIMARY_KEY      | 主键模型或更新模型表的主键。如果该表不是主键模型或更新模型表，则返回空字符串。 |
| PARTITION_KEY    | 表的分区键。                                                 |
| DISTRIBUTE_KEY   | 表的分桶键。                                                 |
| DISTRIBUTE_TYPE  | 表的分桶方式。                                               |
| DISTRIBUTE_BUCKET | 表的分桶数。                                                 |
| SORT_KEY         | 表的排序键。                                                 |
| PROPERTIES       | 表的属性。                                                   |
| TABLE_ID         | 表的 ID。                                                   |

### load_tracking_logs

此功能自 StarRocks v3.0 起支持。

表 `load_tracking_logs` 提供以下字段：

| **字段**         | **描述**                                                     |
| ---------------- | ----------------------------------------------------------- |
| JOB_ID           | 导入作业的 ID。                                               |
| LABEL            | 导入作业的 Label。                                            |
| DATABASE_NAME    | 导入作业所属的数据库名称。                                      |
| TRACKING_LOG     | 导入作业的错误日志信息（如有）。                                 |

### materialized_views

表 `materialized_views` 提供以下字段：

| **字段**                             | **描述**                                         |
| ------------------------------------ | ------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | 物化视图 ID                                      |
| TABLE_SCHEMA                         | 物化视图所在的数据库名称                         |
| TABLE_NAME                           | 物化视图名称                                     |
| REFRESH_TYPE                         | 刷新类型，包括 `ROLLUP`、`ASYNC` 以及 `MANUAL`    |
| IS_ACTIVE                            | 是否生效，失效的物化视图不会被刷新和查询改写          |
| INACTIVE_REASON                      | 失效的原因                                       |
| PARTITION_TYPE                       | 物化视图分区类型                                 |
| TASK_ID                              | 物化视图刷新任务的 ID                            |
| TASK_NAME                            | 物化视图刷新任务的名称                           |
| LAST_REFRESH_START_TIME              | 最近一次刷新任务的开始时间                       |
| LAST_REFRESH_FINISHED_TIME           | 最近一次刷新任务的结束时间                       |
| LAST_REFRESH_DURATION                | 最近一次刷新任务的持续时间                       |
| LAST_REFRESH_STATE                   | 最近一次刷新任务的状态                           |
| LAST_REFRESH_FORCE_REFRESH           | 最近一次刷新任务是否强制刷新                     |
| LAST_REFRESH_START_PARTITION         | 最近一次刷新任务的开始分区                       |
| LAST_REFRESH_END_PARTITION           | 最近一次刷新任务的结束分区                       |
| LAST_REFRESH_BASE_REFRESH_PARTITIONS | 最近一次刷新任务的基表分区                       |
| LAST_REFRESH_MV_REFRESH_PARTITIONS   | 最近一次刷新任务的刷新的分区                     |
| LAST_REFRESH_ERROR_CODE              | 最近一次刷新任务的错误码                         |
| LAST_REFRESH_ERROR_MESSAGE           | 最近一次刷新任务的错误信息                       |
| TABLE_ROWS                           | 物化视图的数据行数，后台统计的近似值             |
| MATERIALIZED_VIEW_DEFINITION         | 物化视图的 SQL 定义                              |
