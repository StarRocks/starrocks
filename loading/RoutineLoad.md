# Routine Load

例行导入（Routine Load）功能，支持提交一个常驻的导入任务，通过不断的从指定的数据源读取数据，将数据导入到 StarRocks 中。 目前仅支持通过无认证或者 SSL 认证方式，从 Kakfa 导入文本格式（CSV）的数据。 本文主要介绍 Routine Load 的使用方式和常见问题。

## 支持的数据格式

* CSV
* JSON

## 基本操作

### 创建导入任务

以从一个本地 Kafka 集群导入 CSV 数据为例：

~~~sql
CREATE ROUTINE LOAD load_test.routine_wiki_edit_1589191587 ON routine_wiki_edit
COLUMNS TERMINATED BY ",",
COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled, delta, added, deleted)
WHERE event_time > "2022-01-01 00:00:00",
PROPERTIES
(
  "desired_concurrent_number" = "3",
  "max_batch_interval" = "15000",
  "max_error_number" = "1000"
)
FROM KAFKA
(
  "kafka_broker_list" = "localhost:9092",
  "kafka_topic" = "starrocks-load"
);
~~~

**参数说明：**

* **job_name**：必填。导入作业的名称，本示例为`routine_wiki_edit_1589191587`。导入作业的常见命名方式为表名+时间戳。在相同数据库内，导入作业的名称不可重复。
  > 您可以在**job_name**前指定导入数据库名称。例如`load_test.routine_wiki_edit_1589191587`。
* **table_name**：必填。导入的目标表的名称。本示例为`routine_wiki_edit`。
* **COLUMN TERMINATED BY 子句**：选填。指定源数据文件中的列分隔符，分隔符默认为：\t。
* **COLUMN 子句** ：选填。用于指定源数据中列和表中列的映射关系。
  * 映射列：如目标表有三列 col1, col2, col3 ，源数据有 4 列，其中第 1、2、4 列分别对应 col2, col1, col3，则书写如下：COLUMNS (col2, col1, temp, col3), ，其中 temp 列为不存在的一列，用于跳过源数据中的第三列。
  * 衍生列：除了直接读取源数据的列内容之外，StarRocks 还提供对数据列的加工操作。假设目标表后加入了第四列 col4 ，其结果由 col1 + col2 产生，则可以书写如下：COLUMNS (col2, col1, temp, col3, col4 = col1 + col2),。
* **WHERE 子句**：过滤条件，只有满足过滤条件的数据才会导入StarRocks中。过滤条件中所指定的列可以是映射列或衍生列。例如，如果仅需要导入 col1 大于 100 并且 col2 等于 1000 的数据，则需要传入`WHERE col1 > 100 and col2 = 1000`。
* **PROPERTIES 子句**：选填。用于指定导入作业的通用参数。
  * **desired_concurrent_number**：导入并发度，指定一个导入作业最多会被分成多少个子任务执行。必须大于 0，默认为 3。子任务最终的个数，由多个参数决定。当前 routine load 并发取决于以下参数：

    ~~~plain text
    min(min(partitionNum,min(desireTaskConcurrentNum,aliveBeNum)),max_routine_load_task_concurrent_num)
    ~~~

    * partitionNum：Kafka 分区数。
    * desireTaskConcurrentNum： desired_concurrent_number 任务配置，参考当前参数释义。
    * aliveBeNum：状态为 Alive 的 BE 节点个数。
    * max_routine_load_task_concurrent_num：be.conf 配置项，默认为5，具体可参考 [参数配置](../administration/Configuration.md)。

  * **max_batch_interval**：每个子任务最大执行时间，单位是「秒」。范围为 5 到 60。默认为 10。**1.15 版本后**: 该参数是子任务的调度间隔，即任务多久执行一次，任务的消费数据时间为 fe.conf 中的 routine_load_task_consume_second，默认为 3s，
  任务的执行超时时间为 fe.conf 中的 routine_load_task_timeout_second，默认为 15s。
  * **max_error_number**：采样窗口内，允许的最大错误行数。必须大于等于 0。默认是 0，即不允许有错误行。注意：被 where 条件过滤掉的行不算错误行。
* **FROM 子句**：指定数据源，以及数据源的相关信息。本示例中数据源为 KAFKA，数据源相关的信息包含如下两项。
  * **kafka_broker_list**：Kafka 的 broker 连接信息，格式为 ip: host。多个 broker 之间以逗号分隔。
  * **kafka_topic**：指定要订阅的 Kafka 的 topic。

创建导入任务更详细的语法可以参考 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/ROUTINE%20LOAD.md)。

### 查看任务状态

显示 [database] 下，所有的例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

~~~SQL
USE [database];
SHOW ALL ROUTINE LOAD;
~~~

显示 [database] 下，名称为 job_name 的当前正在运行的例行导入作业。

~~~SQL
SHOW ROUTINE LOAD FOR [database.][job_name];
~~~

> 注意： StarRocks 只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

查看任务状态的具体命令和示例可以参考 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/SHOW%20ROUTINE%20LOAD.md)。

查看任务运行状态（包括子任务）的具体命令和示例可以参考 [SHOW ROUTINE LOAD TASK](../sql-reference/sql-statements/data-manipulation/SHOW%20ROUTINE%20LOAD%20TASK.md)。

以上述创建的导入任务为示例，以下命令能查看当前正在运行的所有 Routine Load 任务：

~~~sql
MySQL [load_test] > USE load_test;
MySQL [load_test] > SHOW ROUTINE LOAD\G;

*************************** 1. row ***************************

                  Id: 14093
                Name: routine_wiki_edit_1589191587
          CreateTime: 2020-05-16 16:00:48
           PauseTime: N/A
             EndTime: N/A
              DbName: default_cluster:load_test
           TableName: routine_wiki_edit
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","columnToColumnExpr":"event_time,channel,user,is_anonymous,is_minor,is_new,is_robot,is_unpatrolled,delta,added,deleted","maxBatchIntervalS":"10","whereExpr":"*","maxBatchSizeBytes":"104857600","columnSeparator":"','","maxErrorNum":"1000","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"starrocks-load","currentKafkaPartitions":"0","brokerList":"localhost:9092"}
    CustomProperties: {}
           Statistic: {"receivedBytes":150821770,"errorRows":122,"committedTaskNum":12,"loadedRows":2399878,"loadRowsRate":199000,"abortedTaskNum":1,"totalRows":2400000,"unselectedRows":0,"receivedBytesRate":12523000,"taskExecuteTimeMs":12043}
            Progress: {"0":"13634667"}
ReasonOfStateChanged:
        ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_53/error_log_insert_stmt_47e8a1d107ed4932-8f1ddf7b01ad2fee_47e8a1d107ed4932_8f1ddf7b01ad2fee
            OtherMsg:
1 row in set (0.00 sec)
~~~

可以看到示例中创建的名为 routine_wiki_edit_1589191587 的导入任务，其中重要的字段释义：

* State：导入任务状态。RUNNING，表示该导入任务处于持续运行中。
* Statistic 为进度信息，记录了从创建任务开始后的导入信息。
* receivedBytes：接收到的数据大小，单位是「Byte」
* errorRows：导入错误行数
* committedTaskNum：FE 提交的 Task 数
* loadedRows：已导入的行数
* loadRowsRate：导入数据速率，单位是「行每秒(row/s)」
* abortedTaskNum：BE 失败的 Task 数
* totalRows：接收的总行数
* unselectedRows：被 where 条件过滤的行数
* receivedBytesRate：接收数据速率，单位是「Bytes/s」
* taskExecuteTimeMs：导入耗时，单位是「ms」
* Progress：已消费 Kafka 分区的OFFSET
* ErrorLogUrls：错误信息日志，可以通过 URL 看到导入过程中的错误信息。

~~~sql
MySQL [load_test] > USE load_test;
MySQL [load_test] > SHOW ROUTINE LOAD TASK WHERE Jobname="routine_wiki_edit_1589191587"\G;
*************************** 1. row ***************************
              TaskId: 645da10b-0a5c-4e90-84f0-03b33ec58b68
               TxnId: 2776810
           TxnStatus: UNKNOWN
               JobId: 144093
          CreateTime: 2020-05-16 16:00:48
   LastScheduledTime: 2020-05-16 16:01:18
    ExecuteStartTime: NULL
             Timeout: 15
                BeId: 10003
DataSourceProperties: {"0":13634684}
             Message: 
1 rows in set (0.00 sec)
~~~

可以看到示例中创建的名为 routine_wiki_edit_1589191587 的导入子任务，其中重要的字段释义：

* TaskId：子任务 ID
* TxnId：本次导入任务事物 ID
* JobId：任务ID，例如例子中 routine_wiki_edit_1589191587 对应的 ID
* DataSourceProperties：已消费 Kafka 分区的OFFSET

### 暂停导入任务

使用 PAUSE 语句后，此时导入任务进入 PAUSED 状态，数据暂停导入，但任务未消亡，可以通过 RESUME 语句可以重启任务：

暂停名称为 job_name 的例行导入任务。

~~~SQL
PAUSE ROUTINE LOAD FOR [job_name];
~~~

可以参考 [PAUSE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/PAUSE%20ROUTINE%20LOAD.md)

~~~sql
MySQL [load_test] > PAUSE ROUTINE LOAD FOR routine_wiki_edit_1589191587;
Query OK, 0 rows affected (0.01 sec)
MySQL [load_test] > SHOW ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_wiki_edit_1589191587
          CreateTime: 2020-05-16 16:00:48
           PauseTime: 2020-05-16 16:03:39
             EndTime: N/A
              DbName: default_cluster:load_test
           TableName: routine_wiki_edit
               State: PAUSED
      DataSourceType: KAFKA
      CurrentTaskNum: 0
       JobProperties: {"partitions":"*","columnToColumnExpr":"event_time,channel,user,is_anonymous,is_minor,is_new,is_robot,is_unpatrolled,delta,added,deleted","maxBatchIntervalS":"10","whereExpr":"*","maxBatchSizeBytes":"104857600","columnSeparator":"','","maxErrorNum":"1000","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"starrocks-load","currentKafkaPartitions":"0","brokerList":"localhost:9092"}
    CustomProperties: {}
           Statistic: {"receivedBytes":162767220,"errorRows":132,"committedTaskNum":13,"loadedRows":2589972,"loadRowsRate":115000,"abortedTaskNum":7,"totalRows":2590104,"unselectedRows":0,"receivedBytesRate":7279000,"taskExecuteTimeMs":22359}
            Progress: {"0":"13824771"}
ReasonOfStateChanged: ErrorReason{code=errCode = 100, msg='User root pauses routine load job'}
        ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_54/error_log_insert_stmt_e0c0c6b040c044fd-a162b16f6bad53e6_e0c0c6b040c044fd_a162b16f6bad53e6
            OtherMsg:
1 row in set (0.01 sec)
~~~

暂停导入任务后，任务的 State 变更为 PAUSED，Statistic 和 Progress 中的导入信息停止更新。此时，任务并未消亡，通过 SHOW ROUTINE LOAD 语句可以看到已经暂停的导入任务。

### 恢复导入任务

使用 RESUME 语句后，任务会短暂的进入 **NEED_SCHEDULE** 状态，表示任务正在重新调度，一段时间后会重新恢复至 RUNNING 状态，继续导入数据。

重启名称为 job_name 的例行导入任务。

~~~SQL
RESUME ROUTINE LOAD FOR [job_name];
~~~

可以参考 [RESUME ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/RESUME%20ROUTINE%20LOAD.md)

~~~sql
MySQL [load_test] > RESUME ROUTINE LOAD FOR routine_wiki_edit_1589191587;
Query OK, 0 rows affected (0.01 sec)
MySQL [load_test] > SHOW ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_wiki_edit_1589191587
          CreateTime: 2020-05-16 16:00:48
           PauseTime: N/A
             EndTime: N/A
              DbName: default_cluster: load_test
           TableName: routine_wiki_edit
               State: NEED_SCHEDULE
      DataSourceType: KAFKA
      CurrentTaskNum: 0
       JobProperties: {"partitions":"*","columnToColumnExpr":"event_time,channel,user,is_anonymous,is_minor,is_new,is_robot,is_unpatrolled,delta,added,deleted","maxBatchIntervalS":"10","whereExpr":"*","maxBatchSizeBytes":"104857600","columnSeparator":"','","maxErrorNum":"1000","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"starrocks-load","currentKafkaPartitions":"0","brokerList":"localhost:9092"}
    CustomProperties: {}
           Statistic: {"receivedBytes":162767220,"errorRows":132,"committedTaskNum":13,"loadedRows":2589972,"loadRowsRate":115000,"abortedTaskNum":7,"totalRows":2590104,"unselectedRows":0,"receivedBytesRate":7279000,"taskExecuteTimeMs":22359}
            Progress: {"0":"13824771"}
ReasonOfStateChanged:
        ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_54/error_log_insert_stmt_e0c0c6b040c044fd-a162b16f6bad53e6_e0c0c6b040c044fd_a162b16f6bad53e6
            OtherMsg:
1 row in set (0.00 sec)
~~~

~~~sql
MySQL [load_test] > SHOW ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_wiki_edit_1589191587
          CreateTime: 2020-05-16 16:00:48
           PauseTime: N/A
             EndTime: N/A
              DbName: default_cluster:load_test
           TableName: routine_wiki_edit
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","columnToColumnExpr":"event_time,channel,user,is_anonymous,is_minor,is_new,is_robot,is_unpatrolled,delta,added,deleted","maxBatchIntervalS":"10","whereExpr":"*","maxBatchSizeBytes":"104857600","columnSeparator":"','","maxErrorNum":"1000","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"starrocks-load","currentKafkaPartitions":"0","brokerList":"localhost:9092"}
    CustomProperties: {}
           Statistic: {"receivedBytes":175337712,"errorRows":142,"committedTaskNum":14,"loadedRows":2789962,"loadRowsRate":118000,"abortedTaskNum":7,"totalRows":2790104,"unselectedRows":0,"receivedBytesRate":7422000,"taskExecuteTimeMs":23623}
            Progress: {"0":"14024771"}
ReasonOfStateChanged:
        ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_55/error_log_insert_stmt_ce4c95f0c72440ef-a442bb300bd743c8_ce4c95f0c72440ef_a442bb300bd743c8
            OtherMsg:
1 row in set (0.00 sec)
~~~

重启导入任务后，可以看到第一次查询任务时，State 变更为 **NEED_SCHEDULE**，表示任务正在重新调度；第二次查询任务时，State 变更为 **RUNNING**，同时 Statistic 和 Progress 中的导入信息开始更新，继续导入数据。

### 修改导入任务

使用 ALTER 语句可以改变已经创建的例行导入作业的参数，只支持状态为 **PAUSED** 的任务。

以上面创建的任务为例，将 `desired_concurrent_number` 修改为 10，修改 partition 的 offset。

~~~sql
ALTER ROUTINE LOAD FOR routine_wiki_edit
PROPERTIES
(
    "desired_concurrent_number" = "10"
)
FROM kafka
(
    "kafka_partitions" = "0",
    "kafka_offsets" = "16414342",
);
~~~

其他参数详解请参考 [ALTER ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/alter-routine-load.md)

### 停止导入任务

使用 STOP 语句让导入任务进入 STOP 状态，数据停止导入，任务消亡，无法恢复数据导入。

停止名称为 job_name 的例行导入任务。

~~~SQL
STOP ROUTINE LOAD FOR [job_name];
~~~

可以参考 [STOP ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/STOP%20ROUTINE%20LOAD.md)

~~~sql
MySQL [load_test] > STOP ROUTINE LOAD FOR routine_wiki_edit_1589191587;
Query OK, 0 rows affected (0.01 sec)
MySQL [load_test] > SHOW ALL ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_wiki_edit_1589191587
          CreateTime: 2020-05-16 16:00:48
           PauseTime: N/A
             EndTime: 2020-05-16 16:08:25
              DbName: default_cluster: load_test
           TableName: routine_wiki_edit
               State: STOPPED
      DataSourceType: KAFKA
      CurrentTaskNum: 0
       JobProperties: {"partitions":"*","columnToColumnExpr":"event_time,channel,user,is_anonymous,is_minor,is_new,is_robot,is_unpatrolled,delta,added,deleted","maxBatchIntervalS":"10","whereExpr":"*","maxBatchSizeBytes":"104857600","columnSeparator":"','","maxErrorNum":"1000","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"starrocks-load","currentKafkaPartitions":"0","brokerList":"localhost:9092"}
    CustomProperties: {}
           Statistic: {"receivedBytes":325534440,"errorRows":264,"committedTaskNum":26,"loadedRows":5179944,"loadRowsRate":109000,"abortedTaskNum":18,"totalRows":5180208,"unselectedRows":0,"receivedBytesRate":6900000,"taskExecuteTimeMs":47173}
            Progress: {"0":"16414875"}
ReasonOfStateChanged:
        ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_67/error_log_insert_stmt_79e9504cafee4fbd-b3981a65fb158cde_79e9504cafee4fbd_b3981a65fb158cde
            OtherMsg:
~~~

停止导入任务后，任务的 State 变更为 STOP，Statistic 和 Progress 中的导入信息再也不会更新。此时，通过 SHOW ROUTINE LOAD 语句无法看到已经停止的导入任务。

## JSON 文本导入

### json 文本

下面每个`{}`代表一行数据，`[]`中表示有3行数据。

~~~json
[
    {"category": "11", "title": "SayingsoftheCentury", "price": 895, "timestamp": 1589191587},
    {"category": "22", "author": "2avc", "price": 895, "timestamp": 1589191487},
    {"category": "33", "author": "3avc", "title": "SayingsoftheCentury", "timestamp": 1589191387}
]
~~~

### 创建任务

~~~sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "strict_mode" = "false",
    "format" = "json",
    "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
    "strip_outer_array" = "true"
)
FROM KAFKA
(
    "kafka_broker_list" = "localhost:9092",
    "kafka_topic" = "my_topic"
);
~~~

**参数说明：**

* format : json，指定导入的数据类型，只有导入 json 数据的时候需要指定。
* jsonpaths : 选择每一列的 json 路径。
* json_root : 选择 json 开始解析的对象。
* strip_outer_array : 裁剪最外面的 array 字段，默认为 false。当数据为格式为下面数组格式的需要配置为 true，`{"key1": "value1"}` 表示一行数据。

~~~json
[
    {
        "key1": "value1"
    },
    {
        "key2": "value2"
    }
]
~~~

JSON文本暂停、恢复和停止导入任务指令与上述CSV格式一致。

更多详细的事例请参考 [ROUTINE LOAD 导入例子](../sql-reference/sql-statements/data-manipulation/ROUTINE%20LOAD.md#example)

---

## 常见问题

* Q：导入任务被 PAUSE，报错 Broker: Offset out of range。<br>
  A：通过 SHOW ROUTINE LOAD 查看最新的 offset，用 Kafka 客户端查看该 offset 有没有数据。<br>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;可能原因：
  * 导入时指定了未来的 offset。
  * 还没来得及导入，Kafka 已经将该 offset 的数据清理。需要根据 StarRocks 的导入速度设置合理的 log 清理参数 log.retention.hours、log.retention.bytes 等。

* Q：如何提高 ROUTINE LOAD 效率。<br>

  A：当前 ROUNTINE LOAD 并发取决于：

     ~~~plain text
     min(min(partitionNum, min(desireTaskConcurrentNum, aliveBeNum)), Config.max_routine_load_task_concurrent_num)
     ~~~

  * partitionNum：Kafka topic 分区个数
  * desireTaskConcurrentNum： desired_concurrent_number 配置，见 [创建导入任务](#创建导入任务) 参数说明
  * aliveBeNum：集群存活的 BE 节点个数
  * max_routine_load_task_concurrent_num：be.conf 配置项，默认为5，具体可参考 [参数配置](../administration/Configuration.md)

     可以看出来主要受限于存活的 BE 节点个数，您的 **Kafka topic分区数 > BE节点个数** 的时候，建议拆分成多个 ROUTINE LOAD 任务。比如下面这个场景下：

     ~~~plain text
     BE 节点：3 台
     Kafka topic 分区数：6
     ~~~

     可以将您的任务可以拆分成两个 ROUTINE LOAD 任务，可以指定每个任务消费 3 个分区。
