---
displayed_sidebar: docs
---
import Experimental from '../_assets/commonMarkdown/_experimental.mdx'

# 从 Apache® Pulsar™ 持续导入数据

<Experimental />

从 StarRocks 2.5 版本开始，Routine Load 支持从 Apache® Pulsar™ 持续导入数据。Pulsar 是一个分布式的开源发布-订阅消息和流处理平台，具有存储与计算分离的架构。通过 Routine Load 从 Pulsar 导入数据与从 Apache Kafka 导入数据类似。本主题以 CSV 格式的数据为例，介绍如何通过 Routine Load 从 Apache Pulsar 导入数据。

## 支持的数据文件格式

Routine Load 支持从 Pulsar 集群消费 CSV 和 JSON 格式的数据。

> 注意
>
> 对于 CSV 格式的数据，StarRocks 支持使用 UTF-8 编码的字符串（最长 50 字节）作为列分隔符。常用的列分隔符包括逗号（,）、制表符和竖线（|）。

## Pulsar 相关概念

**[Topic](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#topics)**

Pulsar 中的 Topic 是用于从生产者向消费者传输消息的命名通道。Pulsar 中的 Topic 分为分区 Topic 和非分区 Topic。

- **[分区 Topic](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#partitioned-topics)** 是一种特殊类型的 Topic，由多个代理处理，从而允许更高的吞吐量。分区 Topic 实际上是由 N 个内部 Topic 实现的，其中 N 是分区的数量。
- **非分区 Topic** 是一种普通类型的 Topic，仅由一个代理提供服务，这限制了 Topic 的最大吞吐量。

**[Message ID](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#messages)**

消息的 Message ID 是由 [BookKeeper 实例](https://pulsar.apache.org/docs/2.10.x/concepts-architecture-overview/#apache-bookkeeper) 在消息被持久存储时分配的。Message ID 表示消息在账本中的特定位置，并在 Pulsar 集群中是唯一的。

Pulsar 支持消费者通过 consumer.*seek*(*messageId*) 指定初始位置。但与 Kafka 消费者偏移量是一个长整数值相比，Message ID 由四部分组成：`ledgerId:entryID:partition-index:batch-index`。

因此，您无法直接从消息中获取 Message ID。因此，目前 **Routine Load 不支持在从 Pulsar 导入数据时指定初始位置，只支持从分区的开头或结尾消费数据。**

**[Subscription](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#subscriptions)**

订阅是一个命名的配置规则，决定了消息如何传递给消费者。Pulsar 还支持消费者同时订阅多个 Topic。一个 Topic 可以有多个订阅。

订阅的类型在消费者连接时定义，可以通过重新启动所有消费者并使用不同的配置来更改。Pulsar 中有四种订阅类型：

- `exclusive`（默认）*:* 只允许一个消费者连接到订阅。只有一个消费者可以消费消息。
- `shared`: 多个消费者可以连接到同一个订阅。消息以轮询方式分发给消费者，任何给定的消息只传递给一个消费者。
- `failover`: 多个消费者可以连接到同一个订阅。为非分区 Topic 或分区 Topic 的每个分区选择一个主消费者接收消息。当主消费者断开连接时，所有（未确认和后续的）消息将传递给下一个消费者。
- `key_shared`: 多个消费者可以连接到同一个订阅。消息在消费者之间分发，具有相同键或相同排序键的消息只传递给一个消费者。

> 注意：
>
> 目前 Routine Load 使用 exclusive 类型。

## 创建 Routine Load 作业

以下示例描述如何在 Pulsar 中消费 CSV 格式的消息，并通过创建 Routine Load 作业将数据导入 StarRocks。有关详细说明和参考，请参见 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md)。

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
    "pulsar_topic" = "persistent://tenant/namespace/topic-name",
    "pulsar_subscription" = "load-test",
    "pulsar_partitions" = "load-partition-0,load-partition-1",
    "pulsar_initial_positions" = "POSITION_EARLIEST,POSITION_LATEST",
    "property.auth.token" = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD5Y"
);
```

当创建 Routine Load 来消费 Pulsar 的数据时，除了 `data_source_properties` 外，大多数输入参数与消费 Kafka 的数据相同。有关 `data_source_properties` 之外的参数描述，请参见 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md)。

与 `data_source_properties` 相关的参数及其描述如下：

| **参数**                                   | **必需** | **描述**                                                      |
| ------------------------------------------- | ------------ | ------------------------------------------------------------ |
| pulsar_service_url                          | 是          | 用于连接 Pulsar 集群的 URL。格式：`"pulsar://ip:port"` 或 `"pulsar://service:port"`。示例：`"pulsar_service_url" = "pulsar://``localhost:6650``"` |
| pulsar_topic                                | 是          | 订阅的 Topic。示例："pulsar_topic" = "persistent://tenant/namespace/topic-name" |
| pulsar_subscription                         | 是          | 为 Topic 配置的订阅。示例：`"pulsar_subscription" = "my_subscription"` |
| pulsar_partitions, pulsar_initial_positions | 否           | `pulsar_partitions` : 订阅的 Topic 分区。`pulsar_initial_positions`: 由 `pulsar_partitions` 指定的分区的初始位置。初始位置必须与 `pulsar_partitions` 中的分区对应。有效值：`POSITION_EARLIEST`（默认值）：订阅从分区中最早可用的消息开始。`POSITION_LATEST`：订阅从分区中最新可用的消息开始。注意：如果未指定 `pulsar_partitions`，则订阅 Topic 的所有分区。如果同时指定了 `pulsar_partitions` 和 `property.pulsar_default_initial_position`，则 `pulsar_partitions` 的值会覆盖 `property.pulsar_default_initial_position` 的值。如果既未指定 `pulsar_partitions` 也未指定 `property.pulsar_default_initial_position`，则订阅从分区中最新可用的消息开始。示例：`"pulsar_partitions" = "my-partition-0,my-partition-1,my-partition-2,my-partition-3", "pulsar_initial_positions" = "POSITION_EARLIEST,POSITION_EARLIEST,POSITION_LATEST,POSITION_LATEST"` |

Routine Load 支持以下 Pulsar 的自定义参数。

| 参数                                      | 必需   | 描述                                                          |
| ---------------------------------------- | -------- | ------------------------------------------------------------ |
| property.pulsar_default_initial_position | 否       | 当订阅 Topic 的分区时的默认初始位置。该参数在未指定 `pulsar_initial_positions` 时生效。其有效值与 `pulsar_initial_positions` 的有效值相同。示例：`"``property.pulsar_default_initial_position" = "POSITION_EARLIEST"` |
| property.auth.token                      | 否       | 如果 Pulsar 启用了使用安全令牌验证客户端身份，则需要令牌字符串来验证您的身份。示例：`"p``roperty.auth.token" = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD"` |

## 检查导入作业和任务

### 检查导入作业

执行 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) 语句以检查导入作业 `routine_wiki_edit_1` 的状态。StarRocks 返回执行状态 `State`、统计信息（包括总消费行数和总导入行数）`Statistics` 以及导入作业的进度 `progress`。

当您检查从 Pulsar 消费数据的 Routine Load 作业时，除了 `progress` 外，大多数返回的参数与消费 Kafka 的数据相同。`progress` 指的是积压，即分区中未确认的消息数量。

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
DataSourceProperties: {"serviceUrl":"pulsar://localhost:6650","currentPulsarPartitions":"my-partition-0,my-partition-1","topic":"persistent://tenant/namespace/topic-name","subscription":"load-test"}
    CustomProperties: {"auth.token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD"}
           Statistic: {"receivedBytes":5480943882,"errorRows":0,"committedTaskNum":696,"loadedRows":66243440,"loadRowsRate":29000,"abortedTaskNum":0,"totalRows":66243440,"unselectedRows":0,"receivedBytesRate":2400000,"taskExecuteTimeMs":2283166}
            Progress: {"my-partition-0(backlog): 100","my-partition-1(backlog): 0"}
ReasonOfStateChanged: 
        ErrorLogUrls: 
            OtherMsg:
1 row in set (0.00 sec)
```

### 检查导入任务

执行 [SHOW ROUTINE LOAD TASK](../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD_TASK.md) 语句以检查导入作业 `routine_wiki_edit_1` 的导入任务，例如有多少任务正在运行、消费的 Kafka Topic 分区和消费进度 `DataSourceProperties`，以及对应的 Coordinator BE 节点 `BeId`。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD TASK WHERE JobName = "routine_wiki_edit_1" \G
```

## 修改导入作业

在修改导入作业之前，您必须使用 [PAUSE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/PAUSE_ROUTINE_LOAD.md) 语句暂停它。然后，您可以执行 [ALTER ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md)。修改后，您可以执行 [RESUME ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md) 语句恢复它，并使用 [SHOW ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) 语句检查其状态。

当 Routine Load 用于消费 Pulsar 的数据时，除了 `data_source_properties` 外，大多数返回的参数与消费 Kafka 的数据相同。

**请注意以下几点**：

- 在与 `data_source_properties` 相关的参数中，目前仅支持修改 `pulsar_partitions`、`pulsar_initial_positions` 和自定义 Pulsar 参数 `property.pulsar_default_initial_position` 和 `property.auth.token`。参数 `pulsar_service_url`、`pulsar_topic` 和 `pulsar_subscription` 不能修改。
- 如果需要修改要消费的分区和匹配的初始位置，您需要确保在创建 Routine Load 作业时使用 `pulsar_partitions` 指定分区，并且只能修改指定分区的初始位置 `pulsar_initial_positions`。
- 如果在创建 Routine Load 作业时仅指定了 Topic `pulsar_topic`，而未指定分区 `pulsar_partitions`，则可以通过 `pulsar_default_initial_position` 修改 Topic 下所有分区的起始位置。