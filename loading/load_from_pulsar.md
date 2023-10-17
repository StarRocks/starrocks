# 【公测中】从 Apache® Pulsar™ 持续导入

自 StarRocks 2.5 版本，Routine Load 支持持续消费 Apache® Pulsar™ 的消息并导入至 StarRocks 中。Pulsar 是一个分布式消息队列系统，采用存储计算分离架构。

通过 Routine Load 将数据从 Pulsar 导入，与从 Apache Kafka 导入类似。本文以 CSV 格式的数据文件为例介绍如何通过 Routine Load  从 Pulsar 持续导入数据。

## 支持的数据文件格式

Routine Load 目前支持从 Pulsar 集群中消费 CSV、JSON 格式的数据。

> 说明
>
> 对于 CSV 格式的数据，StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。

## Pulsar 相关概念

**[Topic](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#topics)**

Topic 负责存储和发布消息。Producer 往 Topic 中写消息，Consumer 从 Topic 中读消息。Pulsar 的 Topic 分为 Partitioned Topic 和 Non-Partitioned Topic 两类。<br />

- [Partitioned Topic](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#partitioned-topics)：通过多个 Broker 提供服务，可以支持更高的吞吐量。Pulsar 通过多个 Internal Topic 来实现 Partitioned Topic。
- Non-Partitioned Topic：只会有单个 Broker 提供服务，限制了 Topic 的吞吐量。

**[Message ID](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#messages)**

 Message ID 表示消息 ID ，在集群维度是唯一的，类似于 Kafka 的 Offset。Consumer 可以通过 seek 特定的 Message ID 移动消费进度。但是相比于 Kafka 的 Offset 为长整型数值，Pulsar 的 Message ID 由四部分构成:  `ledgerId:entryID:partition-index:batch-index`.

 因此，您无法直接通过消息就能得到 Message ID**。**所以目前暂不支持自定义起始 Position，仅支持从 Partition 开头或者结尾开始消费**。

**[Subscription](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#subscriptions)**

订阅是命名好的配置规则，指导消息如何投递给 Consumer，支持如下四种类型：

- `exclusive` (默认)：一个 Subscription 只能与一个 Consumer 关联，只有这个 Consumer 可以接收到 Topic 的全部消息。
- `shared`：多个 Consumer 可以关联同一个 Subscription，消息按照 round-robin 的方式分发到 Consumer 上。
- `failover`：多个 Consumer 可以关联同一个 Subscription，其中部分 Consumer 会被作为 Master Consumer。 对于 Non-Partitioned Topic， 一个 Topic 会选出一个 Master Consumer。对于 Partitioned Topic，一个 Partition 会选出一个 Master Consumer。Master Consumer 负责消费消息。
- `key_shared`：多个 Consumer 可以关联同一个 Subscription，相同 Key 的消息会被分发到同一个 Consumer。

> **注意**
>
> 目前 Routine Load 使用的是 exclusive 模式。

## 创建导入作业

通过 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md) 语句，向StarRocks 提交一个 Routine Load 导入作业 `routine_wiki_edit_1`，持续消费 Pulsar 集群中 Topic `ordertest1`  的消息， 并且使用 Subscription  `load-test`，指定消费分区为 `load-partition-0`，`load-partition-1`，分区对应的消费起始 Position 分别为 Partition 有数据的位置开始消费、Partition 末尾开始消费。并导入至数据库 `load_test` 的表 `routine_wiki_edit` 中。

```SQL
CREATE ROUTINE LOAD load_test.routine_wiki_edit_1 ON routine_wiki_edit
COLUMNS TERMINATED BY ",",
ROWS TERMINATED BY "\n",
COLUMNS (order_id, pay_dt, customer_name, nationality, temp_gender, price)
WHERE event_time > "2022-01-01 00:00:00",
PROPERTIES
(
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "15000",
    "max_error_number" = "1000"
)
FROM PULSAR
(
    "pulsar_service_url" = "pulsar://localhost:6650",
    "pulsar_topic" = "persistent://tenant/namespace/ordertest1",
    "pulsar_subscription" = "load-test",
    "pulsar_partitions" = "load-partition-0,load-partition-1",
    "pulsar_initial_positions" = "POSITION_EARLIEST,POSITION_LATEST",
    "property.auth.token" = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD5Y"
);
```

消费 Pulsar 中的数据，除了  `data_source_properties` 相关参数外其他参数设置方式与[消费 Kafka](./RoutineLoad.md#导入作业) 时相同。

`data_source_properties` 相关参数以及说明如下表所述。

| **参数**                                        | **是否必填** | **说明**                                                     |
| ----------------------------------------------- | ------------ | ------------------------------------------------------------ |
| `pulsar_service_url`                            | 是           | Pulsar 集群的连接信息，格式为 `pulsar://``ip:port` 或 `pulsar://``service:port`。示例：`"pulsar_service_url" = "pulsar://``localhost:6650``"` |
| `pulsar_topic`                                  | 是           | 所需消费 Topic。示例：`"pulsar_topic" = "persistent://tenant/namespace/topic-name"` |
| `pulsar_subscription`                           | 是           | 所需消费 Topic 对应的 Subscription。示例：`"pulsar_subscription" = "my_subscription"` |
| `pulsar_partitions`、`pulsar_initial_positions` | 是           | `pulsar_partitions` 为需要消费的 Partition。`pulsar_initial_positions`为对应 Partition 的消费起始 Position。每设置一个 Partition 需要设置对应 Partition 的消费起始 Position。 取值范围为：`POSITION_EARLIEST` (默认)：从 Partition 有数据的位置开始消费。`POSITION_LATEST`：从 Partition 末尾开始消费。**说明**如果不指定 `pulsar_partitions`，则会自动展开 Topic 下所有  Partition 进行消费。如果 `pulsar_initial_positions` 和 `property.pulsar_default_initial_position` 都指定，则前者会覆盖后者的配置。如果 `pulsar_initial_positions` 和 `property.pulsar_default_initial_position` 都没有指定，则从 subscription 建立后收到的第一条数据开始消费。示例：`"pulsar_partitions" = "my-partition-0,my-partition-1,my-partition-2,my-partition-3", "pulsar_initial_positions" = "POSITION_EARLIEST,POSITION_EARLIEST,POSITION_LATEST,POSITION_LATEST"` |

Routine Load 还支持自定义 Pulsar 参数，如下表所述。

| **参数**                                   | **是否必填** | **说明**                                                     |
| ------------------------------------------ | ------------ | ------------------------------------------------------------ |
| `property.pulsar_default_initial_position` | 否           | 待消费 partition 的默认消费起始 position。 仅在 `pulsar_initial_positions` 未指定时生效。有效取值与 pulsar_initial_positions 一致。示例：`"property.pulsar_default_initial_position" = "POSITION_EARLIEST"` |
| `property.auth.token`                      | 否           | 用于身份认证和鉴权的 [token](https://pulsar.apache.org/docs/2.10.x/security-overview/)，为一个字符串。示例: `"property.auth.token" = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD"` |

## 查看导入作业和任务

### 查看导入作业

提交 Routine Load 导入作业以后，您可以执行 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD.md) 语句来查看导入作业的信息。例如，您可以通过如下命令查看一个名为 `routine_wiki_edit_1` 的导入作业的信息：

```Plaintext
MySQL [load_test] > SHOW ROUTINE LOAD for routine_wiki_edit_1 \G
*************************** 1. row ***************************
                  Id: 10142
                Name: routine_wiki_edit_1
          CreateTime: 2022-06-29 14:52:55
           PauseTime: 2022-06-29 17:33:53
             EndTime: NULL
              DbName: default_cluster:test_pulsar
           TableName: test1
               State: PAUSED
      DataSourceType: PULSAR
      CurrentTaskNum: 0
       JobProperties: {"partitions":"*","rowDelimiter":"'\n'","partial_update":"false","columnToColumnExpr":"*","maxBatchIntervalS":"10","whereExpr":"*","timezone":"Asia/Shanghai","format":"csv","columnSeparator":"','","json_root":"","strict_mode":"false","jsonpaths":"","desireTaskConcurrentNum":"3","maxErrorNum":"10","strip_outer_array":"false","currentTaskConcurrentNum":"0","maxBatchRows":"200000"}
DataSourceProperties: {"serviceUrl":"pulsar://localhost:6650","currentPulsarPartitions":"my-partition-0,my-partition-1","topic":"persistent://tenant/namespace/ordertest1","subscription":"load-test"}
    CustomProperties: {"auth.token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD"}
           Statistic: {"receivedBytes":5480943882,"errorRows":0,"committedTaskNum":696,"loadedRows":66243440,"loadRowsRate":29000,"abortedTaskNum":0,"totalRows":66243440,"unselectedRows":0,"receivedBytesRate":2400000,"taskExecuteTimeMs":2283166}
            Progress: {"my-partition-0(backlog): 100","my-partition-1(backlog): 0"}
ReasonOfStateChanged: 
        ErrorLogUrls: 
            OtherMsg:
1 row in set (0.00 sec)
```

消费 Pulsar 集群中的数据，除了 `progress` 外其他参数以及含义与[消费 Kafka](./RoutineLoad.md#查看导入作业) 时相同。`progress` 表示被消费 Partition 的 Backlog。

### 查看导入任务

提交 Routine Load 导入作业以后，您可以执行 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD.md)，查看导入作业中一个或多个导入任务的信息。例如，您可以通过如下命令查看一个名为 `routine_wiki_edit_1`的导入作业中一个或多个导入任务的信息。比如当前有多少任务正在运行，消费分区及进度`DataSourceProperties`，以及对应的 Coordinator BE 节点 `BeId`。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD TASK WHERE JobName = "routine_wiki_edit_1" \G
```

消费 Pulsar 集群中的数据，参数以及含义与[消费 Kafka](./RoutineLoad.md#查看导入任务) 时类似。

## 修改导入作业

修改前，您需要先执行 [PAUSE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/PAUSE_ROUTINE_LOAD.md) 暂停导入作业。然后执行 [ALTER ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/alter-routine-load.md) 语句，修改导入作业的参数配置。修改成功后，您需要执行 [RESUME ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/RESUME_ROUTINE_LOAD)，恢复导入作业。然后执行 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD.md) 语句查看修改后的导入作业。

消费 Pulsar 集群中的数据，除了 `data_source_properties` 外的参数修改方式与[消费 Kafka](./RoutineLoad.md#修改导入作业)  时相同。

修改前，您需要先执行 [PAUSE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/PAUSE_ROUTINE_LOAD.md) 暂停导入作业。然后执行 [ALTER ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/alter-routine-load.md) 语句，修改导入作业的参数配置。修改成功后，您需要执行 [RESUME ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/RESUME_ROUTINE_LOAD)，恢复导入作业。然后执行 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD.md) 语句查看修改后的导入作业。

消费 Pulsar 集群中的数据，除了 `data_source_properties` 外的参数修改方式与[消费 Kafka](./RoutineLoad.md#修改导入作业)  时相同。

注意：

- `data_source_properties`相关参数中，目前仅支持修改`pulsar_partitions`、`pulsar_initial_positions`，以及自定义 Pulsar 参数`property.pulsar_default_initial_position` 、 `property.auth.token`。不支持修改 `pulsar_service_url`, `pulsar_topic`, `pulsar_subscription`。
- 如果需要修改待消费 partition 以及对应起始 position，则您需要确保在创建 Routine Load 导入作业时已经使用 `pulsar_partitions` 指定 partition。并且仅支持修改已经指定 partition 的起始 position `pulsar_initial_positions`，不支持新增和删除 partition。
- 如果在创建 Routine Load 导入作业时仅指定 Topic `pulsar_topic`，但是没有指定 partition `pulsar_partitions`，则可以通过 `pulsar_default_initial_position` 修改 topic 下所有 partition 的起始 position。
