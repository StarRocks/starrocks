# Routine Load

Routine Load 是一种例行导入方式，StarRocks通过这种方式支持从Kafka持续不断的导入数据，并且支持通过SQL控制导入任务的暂停、重启、停止。本节主要介绍该功能的基本原理和使用方式。

---

## 名词解释

* **RoutineLoadJob**：用户提交的一个例行导入任务。
* **JobScheduler**：例行导入任务调度器，用于调度和拆分一个 RoutineLoadJob 为多个 Task。
* **Task**：RoutineLoadJob 被 JobScheduler 根据规则拆分的子任务。
* **TaskScheduler**：任务调度器，用于调度 Task 的执行。

---

## 基本原理

![routine load](../assets/4.5.2-1.png)

导入流程如上图：

1. 用户通过支持MySQL协议的客户端向 FE 提交一个Kafka导入任务。
2. FE将一个导入任务拆分成若干个Task，每个Task负责导入指定的一部分数据。
3. 每个Task被分配到指定的 BE 上执行。在 BE 上，一个 Task 被视为一个普通的导入任务，
  通过 Stream Load 的导入机制进行导入。
4. BE导入完成后，向 FE 汇报。
5. FE 根据汇报结果，继续生成后续新的 Task，或者对失败的 Task 进行重试。
6. FE 会不断的产生新的 Task，来完成数据不间断的导入。

---

## 导入示例

### 环境要求

* 支持访问无认证或使用 SSL 方式认证的 Kafka 集群。
* 支持的消息格式为 CSV 文本格式，每一个 message 为一行，且行尾**不包含**换行符。
* 仅支持 Kafka 0.10.0.0(含) 以上版本。

### 创建导入任务

**语法：**

~~~sql
CREATE ROUTINE LOAD [database.][job_name] ON [table_name]
    [COLUMNS TERMINATED BY "column_separator" ,]
    [COLUMNS (col1, col2, ...) ,]
    [WHERE where_condition ,]
    [PARTITION (part1, part2, ...)]
    [PROPERTIES ("key" = "value", ...)]
    FROM [DATA_SOURCE]
    [(data_source_properties1 = 'value1', 
    data_source_properties2 = 'value2', 
    ...)]
~~~

**示例：**

以从一个本地Kafka集群导入数据为例：

~~~sql
CREATE ROUTINE LOAD routine_load_wikipedia ON routine_wiki_edit
COLUMNS TERMINATED BY ",",
COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled, delta, added, deleted)
PROPERTIES
(
    "desired_concurrent_number"="1",
    "max_error_number"="1000"
)
FROM KAFKA
(
    "kafka_broker_list"= "localhost:9092",
    "kafka_topic" = "starrocks-load"
);
~~~

**说明：**

* **job_name**：必填。导入作业的名称，前缀可以携带导入数据库名称，常见命名方式为时间戳+表名。
  单个 database 内，任务名称不可重复。
* **table_name**：必填。导入的目标表的名称。
* **COLUMN TERMINATED子句**：选填。指定源数据文件中的列分隔符，分隔符默认为：\t。
* **COLUMN子句** ：选填。用于指定源数据中列和表中列的映射关系。

  * 映射列：如目标表有三列 col1, col2, col3 ，源数据有4列，其中第1、2、4列分别对应col2, col1, col3，则书写如下：COLUMNS (col2, col1, temp, col3), ，其中 temp 列为不存在的一列，用于跳过源数据中的第三列。
  * 衍生列：除了直接读取源数据的列内容之外，StarRocks还提供对数据列的加工操作。假设目标表后加入了第四列 col4 ，其结果由 col1 + col2 产生，则可以书写如下：COLUMNS (col2, col1, temp, col3, col4 = col1 + col2),。

* **WHERE子句**：选填。用于指定过滤条件，可以过滤掉不需要的行。过滤条件可以指定映射列或衍生列。例如只导入 k1 大于 100 并且 k2 等于 1000 的行，则书写如下：WHERE k1 > 100 and k2 = 1000
* **PARTITION子句**：选填。指定导入目标表的哪些 partition 中，如果不指定，则会自动导入到对应的 partition 中。
* **PROPERTIES子句**：选填。用于指定导入作业的通用参数。

* **desired_concurrent_number**：导入并发度，指定一个导入作业最多会被分成多少个子任务执行。必须大于0，默认为3。
* **max_batch_interval**：每个子任务最大执行时间，单位是「秒」。范围为 5 到 60。默认为10。**1.15版本后**: 该参数是子任务的调度时间，即任务多久执行一次，任务的消费数据时间为fe.conf中的routine_load_task_consume_second，默认为3s，
任务的执行超时时间为fe.conf中的routine_load_task_timeout_second，默认为15s。
* **max_batch_rows**：每个子任务最多读取的行数。必须大于等于200000。默认是200000。**1.15版本后**: 该参数只用于定义错误检测窗口范围，窗口的范围是10 * **max-batch-rows**。
* **max_batch_size**：每个子任务最多读取的字节数。单位是「字节」，范围是 100MB 到 1GB。默认是 100MB。**1.15版本后**: 废弃该参数，任务消费数据的时间为fe.conf中的routine_load_task_consume_second，默认为3s。
* **max_error_number**：采样窗口内，允许的最大错误行数。必须大于等于0。默认是 0，即不允许有错误行。注意：被 where 条件过滤掉的行不算错误行。
* **strict_mode**：是否开启严格模式，默认为开启。如果开启后，非空原始数据的列类型变换如果结果为 NULL，则会被过滤，关闭方式为 "strict_mode" = "false"。
* **timezone**：指定导入作业所使用的时区。默认为使用 Session 的 timezone 参数。该参数会影响所有导入涉及的和时区有关的函数结果。

* **DATA_SOURCE**：指定数据源，请使用KAFKA。
* **data_source_properties**: 指定数据源相关的信息。

  * **kafka_broker_list**：Kafka 的 broker 连接信息，格式为 ip:host。多个broker之间以逗号分隔。
  * **kafka_topic**：指定要订阅的 Kafka 的 topic。
  * **kafka_partitions/kafka_offsets**：指定需要订阅的 kafka partition，以及对应的每个 partition 的起始 offset。
  * **property**：此处的属性，主要是kafka相关的属性，功能等同于kafka shell中 "--property" 参数。

创建导入任务更详细的语法可以通过执行 HELP ROUTINE LOAD; 查看。

### 查看任务状态

* 显示 [database] 下，所有的例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    ~~~SQL
    USE [database];
    SHOW ALL ROUTINE LOAD;
    ~~~

* 显示 [database] 下，名称为 job_name 的当前正在运行的例行导入作业.

    ~~~SQL
    SHOW ROUTINE LOAD FOR [database.][job_name];
    ~~~

> 注意： StarRocks 只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

查看任务状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD;` 命令查看。

查看任务运行状态（包括子任务）的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD TASK;` 命令查看。

以上述创建的导入任务为示例，以下命令能查看当前正在运行的所有Routine Load任务：

~~~sql
MySQL [load_test]> SHOW ROUTINE LOAD\G;

*************************** 1. row ***************************

    Id: 14093

    Name: routine_load_wikipedia

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

  ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_53/error_log_insert_stmt_47e8a1d107ed4932-8f1ddf7b01ad2fee_47e8a1d107ed4932_8f1ddf7b01ad2fee, http://172.26.108.172:9122/api/_load_error_log?file=__shard_54/error_log_insert_stmt_e0c0c6b040c044fd-a162b16f6bad53e6_e0c0c6b040c044fd_a162b16f6bad53e6, http://172.26.108.172:9122/api/_load_error_log?file=__shard_55/error_log_insert_stmt_ce4c95f0c72440ef-a442bb300bd743c8_ce4c95f0c72440ef_a442bb300bd743c8

   OtherMsg:

1 row in set (0.00 sec)
~~~

可以看到示例中创建的名为routine_load_wikipedia的导入任务，其中重要的字段释义：

* State：导入任务状态。RUNNING，表示该导入任务处于持续运行中。
* Statistic为进度信息，记录了从创建任务开始后的导入信息。

* receivedBytes：接收到的数据大小，单位是「Byte」
* errorRows：导入错误行数
* committedTaskNum：FE提交的Task数
* loadedRows：已导入的行数
* loadRowsRate：导入数据速率，单位是「行每秒(row/s)」
* abortedTaskNum：BE失败的Task数
* totalRows：接收的总行数
* unselectedRows：被where条件过滤的行数
* receivedBytesRate：接收数据速率，单位是「Bytes/s」
* taskExecuteTimeMs：导入耗时，单位是「ms」
* ErrorLogUrls：错误信息日志，可以通过URL看到导入过程中的错误信息

### 暂停导入任务

使用PAUSE语句后，此时导入任务进入PAUSED状态，数据暂停导入，但任务未消亡，可以通过RESUME语句可以重启任务：

* 暂停名称为 job_name 的例行导入任务。

    ~~~SQL
    PAUSE ROUTINE LOAD FOR [job_name];
    ~~~

可以通过 `HELP PAUSE ROUTINE LOAD;`命令查看帮助和示例。

~~~sql
MySQL [load_test]> SHOW ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_load_wikipedia
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
    ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_54/error_log_insert_stmt_e0c0c6b040c044fd-a162b16f6bad53e6_e0c0c6b040c044fd_a162b16f6bad53e6, http://172.26.108.172:9122/api/_load_error_log?file=__shard_55/error_log_insert_stmt_ce4c95f0c72440ef-a442bb300bd743c8_ce4c95f0c72440ef_a442bb300bd743c8, http://172.26.108.172:9122/api/_load_error_log?file=__shard_56/error_log_insert_stmt_8753041cd5fb42d0-b5150367a5175391_8753041cd5fb42d0_b5150367a5175391
      OtherMsg:
1 row in set (0.01 sec)
~~~

暂停导入任务后，任务的State变更为PAUSED，Statistic和Progress中的导入信息停止更新。此时，任务并未消亡，通过SHOW ROUTINE LOAD语句可以看到已经暂停的导入任务。

### 恢复导入任务

使用RESUME语句后，任务会短暂的进入 **NEED_SCHEDULE** 状态，表示任务正在重新调度，一段时间后会重新恢复至RUNING状态，继续导入数据。

* 重启名称为 job_name 的例行导入任务。

    ~~~SQL
    RESUME ROUTINE LOAD FOR [job_name];
    ~~~

可以通过 `HELP RESUME ROUTINE LOAD;` 命令查看帮助和示例。

~~~sql
MySQL [load_test]> RESUME ROUTINE LOAD FOR routine_load_wikipedia;
Query OK, 0 rows affected (0.01 sec)
MySQL [load_test]> SHOW ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_load_wikipedia
          CreateTime: 2020-05-16 16:00:48
           PauseTime: N/A
             EndTime: N/A
              DbName: default_cluster:load_test
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
    ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_54/error_log_insert_stmt_e0c0c6b040c044fd-a162b16f6bad53e6_e0c0c6b040c044fd_a162b16f6bad53e6, http://172.26.108.172:9122/api/_load_error_log?file=__shard_55/error_log_insert_stmt_ce4c95f0c72440ef-a442bb300bd743c8_ce4c95f0c72440ef_a442bb300bd743c8, http://172.26.108.172:9122/api/_load_error_log?file=__shard_56/error_log_insert_stmt_8753041cd5fb42d0-b5150367a5175391_8753041cd5fb42d0_b5150367a5175391
        OtherMsg:
1 row in set (0.00 sec)
~~~

~~~sql
MySQL [load_test]> SHOW ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_load_wikipedia
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
    ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_55/error_log_insert_stmt_ce4c95f0c72440ef-a442bb300bd743c8_ce4c95f0c72440ef_a442bb300bd743c8, http://172.26.108.172:9122/api/_load_error_log?file=__shard_56/error_log_insert_stmt_8753041cd5fb42d0-b5150367a5175391_8753041cd5fb42d0_b5150367a5175391, http://172.26.108.172:9122/api/_load_error_log?file=__shard_57/error_log_insert_stmt_31304c87bb82431a-9f2baf7d5fd7f252_31304c87bb82431a_9f2baf7d5fd7f252
            OtherMsg:
1 row in set (0.00 sec)
ERROR: No query specified
~~~

重启导入任务后，可以看到第一次查询任务时，State变更为**NEED_SCHEDULE**，表示任务正在重新调度；第二次查询任务时，State变更为**RUNING**，同时Statistic和Progress中的导入信息开始更新，继续导入数据。

### 停止导入任务

使用STOP语句让导入任务进入STOP状态，数据停止导入，任务消亡，无法恢复数据导入。

* 停止名称为 job_name 的例行导入任务。`

    ~~~SQL
    STOP ROUTINE LOAD FOR [job_name];
    ~~~

可以通过 `HELP STOP ROUTINE LOAD;` 命令查看帮助和示例。

~~~sql
MySQL [load_test]> STOP ROUTINE LOAD FOR routine_load_wikipedia;
Query OK, 0 rows affected (0.01 sec)
MySQL [load_test]> SHOW ALL ROUTINE LOAD\G;
*************************** 1. row ***************************
                  Id: 14093
                Name: routine_load_wikipedia
          CreateTime: 2020-05-16 16:00:48
           PauseTime: N/A
             EndTime: 2020-05-16 16:08:25
              DbName: default_cluster:load_test
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
        ErrorLogUrls: http://172.26.108.172:9122/api/_load_error_log?file=__shard_67/error_log_insert_stmt_79e9504cafee4fbd-b3981a65fb158cde_79e9504cafee4fbd_b3981a65fb158cde, http://172.26.108.172:9122/api/_load_error_log?file=__shard_68/error_log_insert_stmt_b6981319ce56421b-bf4486c2cd371353_b6981319ce56421b_bf4486c2cd371353, http://172.26.108.172:9122/api/_load_error_log?file=__shard_69/error_log_insert_stmt_1121400c1f6f4aed-866c381eb49c966e_1121400c1f6f4aed_866c381eb49c966e
            OtherMsg:
~~~

停止导入任务后，任务的State变更为STOP，Statistic和Progress中的导入信息再也不会更新。此时，通过SHOW ROUTINE LOAD语句无法看到已经停止的导入任务。

---

## 常见问题

* Q：导入任务被PAUSE，报错Broker: Offset out of range

  A：通过SHOW ROUTINE LOAD查看最新的offset，用Kafka客户端查看该offset有没有数据。

    可能原因：

  * 导入时指定了未来的offset。
  * 还没来得及导入，Kafka已经将该offset的数据清理。需要根据StarRocks的导入速度设置合理的log清理参数log.retention.hours、log.retention.bytes等。
