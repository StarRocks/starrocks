---
displayed_sidebar: "English"
---

# SHOW ROUTINE LOAD

import RoutineLoadPrivNote from '../../../assets/commonMarkdown/RoutineLoadPrivNote.md'

## Description

Shows the execution information of Routine Load jobs.

<RoutineLoadPrivNote />

## Syntax

```SQL
SHOW [ALL] ROUTINE LOAD FOR [<db_name>.]<job_name>
[ WHERE [ STATE = { "NEED_SCHEDULE" | "RUNNING" | "PAUSED" | "UNSTABLE" | "STOPPED" | "CANCELLED"  } ] ]
[ ORDER BY field_name [ ASC | DESC ] ]
[ LIMIT { [offset, ] limit | limit OFFSET offset } ]
```

:::tip

You can add the `\G` option to the statement (such as `SHOW ROUTINE LOAD FOR <job_name>\G`) to vertically display the return result rather than in the usual horizontal table format.

:::

## Parameters

| **Parameter**                     | **Required** | **Description**                                              |
| --------------------------------- | ------------ | ------------------------------------------------------------ |
| db_name                           | No           | The name of the database to which the load job belongs.      |
| job_name                          | Yes          | The name of the load job.                                    |
| ALL                               | No           | Displays all load jobs, including those in the `STOPPED` or  `CANCELLED` states. |
| STATE                             | No           |  The status of the load job.                                       |
| ORDER BY field_name [ASC \| DESC] | No           | Sorts the return result in ascending or descending order based on the specified field. The following fields are supported: `Id`, `Name`, `CreateTime`, `PauseTime`, `EndTime`, `TableName`, `State`, and `CurrentTaskNum`.<ul><li>To sort the return result in ascending order, specify `ORDER BY field_name ASC`.</li><li>To sort the return result in descending order, specify `ORDER BY field_name DESC`.</li></ul>If you do not specify the field or the sort order, the return result is sorted in ascending order of `Id` by default. |
| LIMIT limit                       | No           | The number of load jobs that will be returned. For example, if `LIMIT 10` is specified, only the information of 10 load jobs that match filter conditions will be returned. If this parameter is not specified, the information of all load jobs that match the filter conditions are displayed.  |
| OFFSET offset                     | No           | The `offset` parameter defines the number of load jobs to be skipped. For example, `OFFSET 5` skips the first five load jobs and returns the rest. The value of the `offset` parameter defaults to `0`. |

## Output

| **Parameter**        | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| Id                   | The globally unique ID of the load job, generated automatically by StarRocks. |
| Name                 | The name of the load job.                                        |
| CreateTime           | The date and time when the load job was created.             |
| PauseTime            | The date and time when the load job entered `PAUSED` state.  |
| EndTime              | The date and time when the load job entered `STOPPED` state. |
| DbName               | The database to which the target table of the load job belongs.  |
| TableName            | The target table of the load job.                                |
| State                | The status of the load job, including:<ul><li>`NEED_SCHEDULE`: The load job is waiting to be scheduled. After you use CREATE ROUTINE LOAD or RESUME ROUTINE LOAD to create or resume a Routine Load job, the load job first enters the `NEED_SCHEDULE` state.</li><li>`RUNNING`: The load job is running. You can view the consumption progress of the Routine Load job through `Statistic` and `Progress`.</li><li>`PAUSED`: The load job is paused. You can refer to `ReasonOfStateChanged` and `ErrorLogUrls` for troubleshooting. After fixing the error, you can use RESUME ROUTINE LOAD to resume the Routine Load job.</li><li>`CANCELLED`: The load job is cancelled. You can refer to `ReasonOfStateChanged` and `ErrorLogUrls` for troubleshooting. However, after fixing the error, you cannot recover the load job in this state.</li><li>`STOPPED`: The load job is stopped. You cannot recover the load job in this state.</li><li>`UNSTABLE`: The load job is unstable. A Routine Load job is set in the `UNSTABLE` state if any task within the Routine Load job lags (namely, the difference between the timestamp of the message being consumed and the current time exceeds this FE parameter [`routine_load_unstable_threshold_second`](../../../administration/Configuration.md#routine_load_unstable_threshold_second), and unconsumed messages exist in the data source.)</li></ul> |
| DataSourceType       | The type of the data source. Fixed value: `KAFKA`.           |
| CurrentTaskNum       | The current number of tasks in the load job.                     |
| JobProperties        | The properties of the load job, such as partitions to be consumed and column mapping. |
| DataSourceProperties | The properties of the data source, such as Topic and the list of addresses and ports of brokers in the Kafka cluster. |
| CustomProperties     | The additional data source-related properties defined in the load job. |
| Statistic            | The statistic of loading data, such as successfully loaded rows, total rows, and received data volume. |
| Progress             | The progress (measured in the offset) of consuming messages in partitions of the topic. |
| TimestampProgress    | The progress (measured in the timestamp) of consuming messages in partitions of the topic. |
| ReasonOfStateChanged | The reasons for the load job being in CANCELLED or PAUSED state. |
| ErrorLogUrls         | The URL of error logs. You can use the `curl` or `wget` command to  access the URL. |
| TrackingSQL          | The SQL command that you can directly run to query the error log information recorded in the `information_schema` database. |
| OtherMsg             | The information about all failed load tasks of the Routine Load job. |
| LatestSourcePosition | The latest consumer position of messages in partitions of the topic. |

## Examples

If a load job is successfully started and stays in the RUNNING state, the returned result might be as follows:

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

If a load job is in the `PAUSED` or `CANCELLED` state due to exceptions, you can troubleshoot based on the `ReasonOfStateChanged`, `ErrorLogUrls`, `TrackingSQL`, and `OtherMsg` fields in the return result.

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
