---
displayed_sidebar: "Chinese"
---

# SHOW ROUTINE LOAD

## 功能

查看 Routine Load 导入作业的执行情况。

## 语法

```SQL
SHOW [ALL] ROUTINE LOAD FOR [<db_name>.]<job_name>
[ WHERE [ STATE = { "NEED_SCHEDULE" | "RUNNING" | "PAUSED" | "UNSTABLE" | "STOPPED" | "CANCELLED"  } ] ]
[ ORDER BY <field_name> [ ASC | DESC ] ]
[ LIMIT { [offset, ] limit | limit OFFSET offset } ]
```

:::note

如果返回结果中的字段较多，可使用 `\G` 分行，如 `SHOW ROUTINE LOAD FOR <job_name>\G`。

:::

## 参数说明

| **参数**                          | **必选** | **说明**                                                     |
| --------------------------------- | -------- | ------------------------------------------------------------ |
| db_name                           | 否       | 导入作业所属数据库名称。                                     |
| job_name                          | ✅        | 导入作业名称。                                               |
| ALL                               | 否       | 显示所有导入作业，包括处于 `STOPPED` 和 `CANCELLED` 状态的导入作业。 |
| STATE                             | 否       | 导入作业状态。                                               |
| ORDER BY field_name [ASC \| DESC] | 否       | 将返回结果按照指定字段升序或降序排列，当前支持的排序字段（`field_name`）包括 `Id`、`Name`、`CreateTime`、`PauseTime`、`EndTime`、`TableName`、`State` 和 `CurrentTaskNum`。如要升序排列，指定 `ORDER BY field_name ASC`。如要降序排列，指定 `ORDER BY field_name DESC`。如既不指定排序字段也不指定排列顺序，则默认按照 `Id` 升序排列。 |
| LIMIT limit                       | 否       | 查看指定数量的导入作业，例如 `LIMIT 10` 会显示 10 个符合筛选条件的导入作业。如不指定该参数，则默认显示所有符合筛选条件的导入作业。 |
| OFFSET offset                     | 否       | `offset` 定义了返回结果中跳过的导入作业的数量，其默认值为 0。例如 `OFFSET 5` 表示跳过前 5 个导入作业，返回剩下的结果。 |

## 返回结果说明

| **参数**             | **说明**                                                     |
| -------------------- | ------------------------------------------------------------ |
| Id                   | 导入作业的全局唯一 ID，由 StarRocks 自动生成。               |
| Name                 | 导入作业的名称。                                             |
| CreateTime           | 导入作业创建的时间。                                         |
| PauseTime            | 处于 `PAUSED` 状态的导入作业的暂停时间。                     |
| EndTime              | 处于 `STOPPED` 状态的导入作业的停止时间。                    |
| DbName               | 导入作业目标表所属数据库。                                   |
| TableName            | 导入作业目标表。                                             |
| State                | 导入作业状态。包括：<ul><li>`NEED_SCHEDULE`：待调度状态。CREATE ROUTINE LOAD 或者 RESUME ROUTINE LOAD 后，导入作业会先进入待调度状态。</li><li>`RUNNING`：运行状态。消费进度可以通过`Statistic`、`Progress` 中查看。</li><li>`PAUSED`：暂停状态。您可以参考 `ReasonOfStateChanged`、`ErrorLogUrls` 进行修复。修复后您可以使用 [RESUME ROUTINE LOAD](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-statements/data-manipulation/RESUME_ROUTINE_LOAD/)。</li><li>`CANCELLED`：取消状态。您可以参考 `ReasonOfStateChanged`、`ErrorLogUrls` 进行修复。但是修复后，您无法恢复该状态的导入作业。</li><li>`STOPPED`：停止状态。您无法恢复该状态的导入作业。</li><li>`UNSTABLE`：不稳定状态。Routine Load 导入作业的任一导入任务消费延迟，即正在消费的消息时间戳与当前时间的差值超过 FE 参数 [`routine_load_unstable_threshold_second`](../../../administration/Configuration.md#routine_load_unstable_threshold_second)  的值，且数据源中存在未被消费的消息，则导入作业置为 `UNSTABLE` 状态。 </li></ul>|
| DataSourceType       | 数据源类型。固定为 `KAFKA`。                                 |
| CurrentTaskNum       | 导入作业当前的任务数量。                                     |
| JobProperties        | 导入作业属性。比如消费分区、列映射关系。                     |
| DataSourceProperties | 数据源属性。比如 Topic、Kafka 集群中 Broker 的地址和端口列表。 |
| CustomProperties     | 导入作业中自定义的更多数据源相关属性。                       |
| Statistic            | 导入数据的指标，比如成功导入数据行、总数据行、已接受的数据量。 |
| Progress             | Topic 中各个分区消息的消费进度（以位点进行衡量）。           |
| TimestampProgress    | Topic 中各个分区消息的消费进度（以时间戳进行衡量）。         |
| ReasonOfStateChanged | 导致导入作业处于 `CANCELLED` 或者 `PAUSED` 状态的原因。      |
| ErrorLogUrls         | 错误日志 URL。您可以使用 `curl` 或 `wget` 命令打开该地址。   |
| TrackingSQL          | 直接通过 SQL 查询 Information Schema 中记录的错误日志信息。  |
| OtherMsg             | 所有失败的导入任务的信息。                                   |
| LatestSourcePosition | Topic 中各个分区中消息的最新消费位点。                       |

## 示例

如果导入作业成功处于 `RUNNING` 状态，则返回结果可能如下：

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD FOR example_tbl_ordertest1\G
*************************** 1. row ***************************
                  Id: 10204
                Name: example_tbl_ordertest1
          CreateTime: 2023-12-21 21:01:31
           PauseTime: NULL
             EndTime: NULL
              DbName: example_db
           TableName: example_tbl
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","rowDelimiter":"\t","partial_update":"false","columnToColumnExpr":"order_id,pay_dt,customer_name,nationality,temp_gender,price","maxBatchIntervalS":"10","partial_update_mode":"null","whereExpr":"*","timezone":"Asia/Shanghai","format":"csv","columnSeparator":"','","log_rejected_record_num":"0","taskTimeoutSecond":"60","json_root":"","maxFilterRatio":"1.0","strict_mode":"false","jsonpaths":"","taskConsumeSecond":"15","desireTaskConcurrentNum":"5","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"lilyliuyitest4csv","currentKafkaPartitions":"0","brokerList":"xxx.xx.xx.xxx:9092"}
    CustomProperties: {"kafka_default_offsets":"OFFSET_BEGINNING","group.id":"example_tbl_ordertest1_b05da08f-9b9d-4fe1-b1f2-25d7116d617c"}
           Statistic: {"receivedBytes":313,"errorRows":0,"committedTaskNum":1,"loadedRows":6,"loadRowsRate":0,"abortedTaskNum":0,"totalRows":6,"unselectedRows":0,"receivedBytesRate":0,"taskExecuteTimeMs":699}
            Progress: {"0":"5"}
   TimestampProgress: {"0":"1686143856061"}
ReasonOfStateChanged: 
        ErrorLogUrls: 
         TrackingSQL: 
            OtherMsg: 
LatestSourcePosition: {"0":"6"}
1 row in set (0.01 sec)
```

如果导入作业因异常而处于 `PAUSED` 或者 `CANCELLED` 状态，您可以根据返回结果中的 `ReasonOfStateChanged`、`ErrorLogUrls` 、`TrackingSQL` 和 `OtherMsg` 排查具体原因。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD FOR example_tbl_ordertest2\G
*************************** 1. row ***************************
                  Id: 10204
                Name: example_tbl_ordertest2
          CreateTime: 2023-12-22 12:13:18
           PauseTime: 2023-12-22 12:13:38
             EndTime: NULL
              DbName: example_db
           TableName: example_tbl
               State: PAUSED
      DataSourceType: KAFKA
      CurrentTaskNum: 0
       JobProperties: {"partitions":"*","rowDelimiter":"\t","partial_update":"false","columnToColumnExpr":"order_id,pay_dt,customer_name,nationality,temp_gender,price","maxBatchIntervalS":"10","partial_update_mode":"null","whereExpr":"*","timezone":"Asia/Shanghai","format":"csv","columnSeparator":"','","log_rejected_record_num":"0","taskTimeoutSecond":"60","json_root":"","maxFilterRatio":"1.0","strict_mode":"false","jsonpaths":"","taskConsumeSecond":"15","desireTaskConcurrentNum":"5","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"mytest","currentKafkaPartitions":"0","brokerList":"xxx.xx.xx.xxx:9092"}
    CustomProperties: {"kafka_default_offsets":"OFFSET_BEGINNING","group.id":"example_tbl_ordertest2_b3fada0f-6721-4ad1-920d-e4bf6d6ea7f7"}
           Statistic: {"receivedBytes":541,"errorRows":10,"committedTaskNum":1,"loadedRows":6,"loadRowsRate":0,"abortedTaskNum":0,"totalRows":16,"unselectedRows":0,"receivedBytesRate":0,"taskExecuteTimeMs":646}
            Progress: {"0":"19"}
   TimestampProgress: {"0":"1702623900871"}
ReasonOfStateChanged: ErrorReason{errCode = 102, msg='current error rows is more than max error num'}
        ErrorLogUrls: http://xxx.xx.xx.xxx:8040/api/_load_error_log?file=error_log_b25dcc7e642344b2_b0b342b9de0567db
         TrackingSQL: select tracking_log from information_schema.load_tracking_logs where job_id=10204
            OtherMsg: 
LatestSourcePosition: {"0":"20"}
1 row in set (0.00 sec)
```
>>>>>>> 2953992c4a ([Doc] update routine load sql stmt (#37810))
