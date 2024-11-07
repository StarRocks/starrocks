---
displayed_sidebar: docs
---

# [Preview] Continuously load data from Apache® Pulsar™

As of StarRocks version 2.5, Routine Load supports continuously loading data from Apache® Pulsar™. Pulsar is distributed, open source pub-sub messaging and streaming platform with a store-compute separation architecture. Loading data from Pulsar via Routine Load is similar to loading data from Apache Kafka. This topic uses CSV-formatted data as an example to introduce how to load data from Apache Pulsar via Routine Load.

## Supported data file formats

Routine Load supports consuming CSV and JSON formatted data from a Pulsar cluster.

> NOTE
>
> As for data in CSV format, StarRocks supports UTF-8 encoded strings within 50 bytes as column separators. Commonly used column separators include comma (,), tab and pipe (|).

## Pulsar-related concepts

**[Topic](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#topics)**
  
Topics in Pulsar are named channels for transmitting messages from producers to consumers. Topics in Pulsar are divided into partitioned topics and non-partitioned topics.

- **[Partitioned topics](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#partitioned-topics)** are a special type of topic that are handled by multiple brokers, thus allowing for higher throughput. A partitioned topic is actually implemented as N internal topics, where N is the number of partitions.
- **Non-partitioned topics** are a normal type of topic that are served only by a single broker, which limits the maximum throughput of the topic.

**[Message ID](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#messages)**

The message ID of a message is assigned by [BookKeeper instances](https://pulsar.apache.org/docs/2.10.x/concepts-architecture-overview/#apache-bookkeeper) as soon as the message is persistently stored. Message ID indicates a message' s specific position in a ledger and is unique within a Pulsar cluster.

Pulsar supports consumers specifying the initial position through consumer.*seek*(*messageId*). But compared to the Kafka consumer offset which is a long integer value, the message ID consists of four parts: `ledgerId:entryID:partition-index:batch-index`.

Therefore, you can't get the Message ID directly from the message. As a result, at present, **Routine Load does not support specifying initial position when loading data from Pulsar, but only supports consuming data from the beginning or end of a partition.**

**[Subscription](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#subscriptions)**

A subscription is a named configuration rule that determines how messages are delivered to consumers. Pulsar also supports consumers simultaneously subscribing to multiple topics. A topic can have multiple subscriptions.

The type of a subscription is defined when a consumer connects to it, and the type can be changed by restarting all consumers with a different configuration. Four subscription types are available in Pulsar:

- `exclusive` (default)*:* Only a single consumer is allowed to attach to the subscription. Only one customer is allowed to consume messages.
- `shared`: Multiple consumers can attach to the same subscription. Messages are delivered in a round robin distribution across consumers, and any given message is delivered to only one consumer.
- `failover`: Multiple consumers can attach to the same subscription. A master consumer is picked for a non-partitioned topic or each partition of a partitioned topic and receives messages. When the master consumer disconnects, all (non-acknowledged and subsequent) messages are delivered to the next consumer in line.
- `key_shared`: Multiple consumers can attach to the same subscription. Messages are delivered in a distribution across consumers and message with same key or same ordering key are delivered to only one consumer.

> Note:
>
> Currently Routine Load uses exclusive type.

## Create a Routine Load job

The following examples describe how to consume CSV-formatted messages in Pulsar, and load the data into StarRocks by creating a Routine Load job. For detailed instruction and reference, see [CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md).

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

When Routine Load is created to consume data from Pulsar, most input parameters except for `data_source_properties` are the same as consuming data from Kafka . For descriptions about parameters except data_source_properties `data_source_properties` , see [CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md).

The parameters related to `data_source_properties` and their descriptions are as follows:

| **Parameter**                               | **Required** | **Description**                                              |
| ------------------------------------------- | ------------ | ------------------------------------------------------------ |
| pulsar_service_url                          | Yes          | The URL that is used to connect to the Pulsar cluster.  Format: `"pulsar://ip:port"` or `"pulsar://service:port"`.Example: `"pulsar_service_url" = "pulsar://``localhost:6650``"` |
| pulsar_topic                                | Yes          | Subscribed topic. Example: "pulsar_topic" = "persistent://tenant/namespace/topic-name" |
| pulsar_subscription                         | Yes          | Subscription configured for the topic.Example: `"pulsar_subscription" = "my_subscription"` |
| pulsar_partitions, pulsar_initial_positions | No           | `pulsar_partitions` :  Subscribed partitions in the topic.`pulsar_initial_positions`: initial positions of partitions specified by `pulsar_partitions`. The initial positions must correspond to the partitions in `pulsar_partitions`. Valid values:`POSITION_EARLIEST` (Default value): Subscription starts from the earliest available message in the partition. `POSITION_LATEST`: Subscription starts from the latest available message in the partition.Note:If `pulsar_partitions` is not specified, the topic's all partitions are subscribed.If both `pulsar_partitions` and `property.pulsar_default_initial_position` are specified, the `pulsar_partitions` value overrides `property.pulsar_default_initial_position` value.If neither `pulsar_partitions` nor `property.pulsar_default_initial_position` is specified, subscription starts from the latest available message in the partition.Example:`"pulsar_partitions" = "my-partition-0,my-partition-1,my-partition-2,my-partition-3", "pulsar_initial_positions" = "POSITION_EARLIEST,POSITION_EARLIEST,POSITION_LATEST,POSITION_LATEST"` |

Routine Load supports the following custom parameters for Pulsar.

| Parameter                                | Required | Description                                                  |
| ---------------------------------------- | -------- | ------------------------------------------------------------ |
| property.pulsar_default_initial_position | No       | The default initial positions when the topic's partitions are subscribed. The parameter takes effect when `pulsar_initial_positions` is not specified. Its valid values are the same as the valid values of `pulsar_initial_positions`.Example: `"``property.pulsar_default_initial_position" = "POSITION_EARLIEST"` |
| property.auth.token                      | No       | If Pulsar enables authenticating clients using security tokens, you need the token string to verify your identity.Example: `"p``roperty.auth.token" = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9.lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD"` |

## Check a load job and task

### Check a load job

Execute the [SHOW ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) statement to check the status of the load job `routine_wiki_edit_1`. StarRocks returns the execution state `State`, the statistical information (including the total rows consumed and the total rows loaded) `Statistics`, and the progress of the load job `progress`.

When you check a Routine Load job that consumes data from Pulsar, most returned parameters except for `progress` are the same as consuming data from Kafka.  `progress` refers to backlog, that is the number of unacked messages in a partition.

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

### Check a load task

Execute the [SHOW ROUTINE LOAD TASK](../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD_TASK.md) statement to check the load tasks of the load job `routine_wiki_edit_1`, such as how many tasks are running, the Kafka topic partitions that are consumed and the consumption progress `DataSourceProperties`, and the corresponding Coordinator BE node `BeId`.

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD TASK WHERE JobName = "routine_wiki_edit_1" \G
```

## Alter a load job

Before altering a load job, you must pause it by using the [PAUSE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/PAUSE_ROUTINE_LOAD.md) statement. Then you can execute the [ALTER ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md). After altering it, you can execute the [RESUME ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md) statement to resume it, and check its status by using the [SHOW ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) statement.

When Routine Load is used to consume data from Pulsar, most returned parameters except for `data_source_properties` are the same as consuming data from Kafka.

**Take note of the following points**:

- Among the `data_source_properties` related parameters, only `pulsar_partitions`, `pulsar_initial_positions`, and custom Pulsar parameters `property.pulsar_default_initial_position` and `property.auth.token` are currently supported to be modified.  The parameters `pulsar_service_url`, `pulsar_topic`, and `pulsar_subscription` cannot be modified.
- If you need to modify the partition to be consumed and the matched initilal position, you need to make sure that you specify the partition using `pulsar_partitions` when you create the Routine Load job, and only the intial position `pulsar_initial_positions` of the specified partition can be modified.
- If you specify only Topic `pulsar_topic` when creating a Routine Load job, but not partitions `pulsar_partitions`, you can modify the starting position of all partitions under topic via `pulsar_default_initial_position`.
