---
displayed_sidebar: docs
---

# loads

提供导入作业的结果信息。此视图自 StarRocks v3.1 版本起支持。

`loads` 提供以下字段：

| 字段                | 描述                                                      |
| -------------------- | ------------------------------------------------------------ |
| ID                   | 全局唯一标识符。                                  |
| LABEL                | 导入作业的标签。                                       |
| PROFILE_ID           | Profile 的 ID，可以通过 `ANALYZE PROFILE` 进行分析。 |
| DB_NAME              | 目标表所属的数据库。              |
| TABLE_NAME           | 目标表。                                            |
| USER                 | 启动导入作业的用户。                         |
| WAREHOUSE            | 导入作业所属的仓库。                 |
| STATE                | 导入作业的状态。有效值：<ul><li>`PENDING`/`BEGIN`：导入作业已创建。</li><li>`QUEUEING`/`BEFORE_LOAD`：导入作业在队列中等待调度。</li><li>`LOADING`：导入作业正在运行。</li><li>`PREPARING`：事务正在预提交。</li><li>`PREPARED`：事务已预提交。</li><li>`COMMITED`：事务已提交。</li><li>`FINISHED`：导入作业成功。</li><li>`CANCELLED`：导入作业失败。</li></ul> |
| PROGRESS             | 导入作业的 ETL 阶段和 LOADING 阶段的进度。 |
| TYPE                 | 导入作业的类型。对于 Broker Load，返回值为 `BROKER`。对于 INSERT，返回值为 `INSERT`。对于 Stream Load，返回值为 `STREAM`。对于 Routine Load，返回值为 `ROUTINE`。 |
| PRIORITY             | 导入作业的优先级。有效值：`HIGHEST`、`HIGH`、`NORMAL`、`LOW` 和 `LOWEST`。 |
| SCAN_ROWS            | 扫描的数据行数。                    |
| SCAN_BYTES           | 扫描的字节数。                        |
| FILTERED_ROWS        | 由于数据质量不佳而被过滤掉的数据行数。 |
| UNSELECTED_ROWS      | 由于 WHERE 子句中指定的条件而被过滤掉的数据行数。 |
| SINK_ROWS            | 加载的数据行数。                     |
| RUNTIME_DETAILS      | 加载运行时元数据。详情请参见 [RUNTIME_DETAILS](#runtime_details)。 |
| CREATE_TIME          | 导入作业创建的时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | 导入作业 LOADING 阶段的开始时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| LOAD_COMMIT_TIME     | 加载事务提交的时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | 导入作业 LOADING 阶段的结束时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| PROPERTIES           | 导入作业的静态属性。详情请参见 [PROPERTIES](#properties)。 |
| ERROR_MSG            | 导入作业的错误信息。如果导入作业未遇到任何错误，则返回 `NULL`。 |
| TRACKING_SQL         | 可用于查询导入作业跟踪日志的 SQL 语句。仅当导入作业涉及不合格数据行时，才返回 SQL 语句。如果导入作业不涉及任何不合格数据行，则返回 `NULL`。 |
| REJECTED_RECORD_PATH | 可以从中访问导入作业中过滤掉的所有不合格数据行的路径。记录的不合格数据行数由导入作业中配置的 `log_rejected_record_num` 参数决定。可以使用 `wget` 命令访问该路径。如果导入作业不涉及任何不合格数据行，则返回 `NULL`。 |

## RUNTIME_DETAILS

- 通用指标：

| 指标               | 描述                                                  |
| -------------------- | ------------------------------------------------------------ |
| load_id              | 加载执行计划的全局唯一 ID。               |
| txn_id               | 加载事务 ID。                                         |

- Broker Load、INSERT INTO 和 Spark Load 的特定指标：

| 指标               | 描述                                                  |
| -------------------- | ------------------------------------------------------------ |
| etl_info             | ETL 详情。此字段仅对 Spark Load 作业有效。对于其他类型的导入作业，值将为空。 |
| etl_start_time       | 导入作业 ETL 阶段的开始时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| etl_start_time       | 导入作业 ETL 阶段的结束时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| unfinished_backends  | 未完成执行的 BE 列表。                      |
| backends             | 参与执行的 BE 列表。                      |
| file_num             | 读取的文件数量。                                        |
| file_size            | 读取的文件总大小。                                    |
| task_num             | 子任务的数量。                                          |

- Routine Load 的特定指标：

| 指标               | 描述                                                  |
| -------------------- | ------------------------------------------------------------ |
| schedule_interval    | Routine Load 的调度间隔。               |
| wait_slot_time       | Routine Load 任务等待执行槽的时间。 |
| check_offset_time    | Routine Load 任务调度期间检查偏移信息所消耗的时间。 |
| consume_time         | Routine Load 任务读取上游数据所消耗的时间。 |
| plan_time            | 生成执行计划的时间。                      |
| commit_publish_time  | 执行 COMMIT RPC 所消耗的时间。                     |

- Stream Load 的特定指标：

| 指标                 | 描述                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | 导入任务的超时时间。                                    |
| begin_txn_ms           | 开始事务所消耗的时间。                    |
| plan_time_ms           | 生成执行计划的时间。                    |
| receive_data_time_ms   | 接收数据的时间。                                   |
| commit_publish_time_ms | 执行 COMMIT RPC 所消耗的时间。                   |
| client_ip              | 客户端 IP 地址。                                         |

## PROPERTIES

- Broker Load、INSERT INTO 和 Spark Load 的特定属性：

| 属性               | 描述                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | 导入任务的超时时间。                                    |
| max_filter_ratio       | 由于数据质量不佳而被过滤掉的数据行的最大比例。 |

- Routine Load 的特定属性：

| 属性               | 描述                                                |
| ---------------------- | ---------------------------------------------------------- |
| job_name               | Routine Load 作业名称。                                     |
| task_num               | 实际并行执行的子任务数量。          |
| timeout                | 导入任务的超时时间。                                    |
