---
displayed_sidebar: "Chinese"
---

# 使用 Kafka connector 导入数据

StarRocks 提供  Apache Kafka®  连接器 (StarRocks Connector for Apache Kafka®)，持续消费 Kafka 的消息并导入至 StarRocks 中。

使用 Kafka connector 可以更好的融入 Kafka 生态，StarRocks 可以与 Kafka Connect 无缝对接。为 StarRocks 准实时接入链路提供了更多的选择。相比于 Routine Load，您可以在以下场景中优先考虑使用 Kafka connector 导入数据：

- 相比于 Routine Load 仅支持导入 CSV、JSON、Avro 格式的数据，Kafka connector 支持导入更丰富的数据格式。只要数据能通过 Kafka connect 的 converters 转换成 JSON 和 CSV 格式，就可以通过 Kafka connector 导入，例如 Protobuf 格式的数据。
- 需要对数据做自定义的 transform 操作，例如 Debezium CDC 格式的数据。
- 从多个 Kafka Topic 导入数据。
- 从 Confluent cloud 导入数据。
- 需要更精细化的控制导入的批次大小，并行度等参数，以求达到导入速率和资源使用之间的平衡。

## 环境准备

### 准备 Kafka 环境

支持自建 Apache Kafka 集群和 Confluent cloud：

- 如果使用自建 Apache Kafka 集群，请确保已部署 Apache Kafka 集群和 Kafka Connect 集群，并创建 Topic。
- 如果使用 Confluent cloud，请确保已拥有 Confluent 账号，并已经创建集群和 Topic。

### 安装 Kafka connector

安装 Kafka connector 至 Kafka connect。

- 自建 Kafka 集群

  - 下载并解压压缩包 [starrocks-kafka-connector](https://github.com/StarRocks/starrocks-connector-for-apache-flink/releases)。
  - 将解压后的目录复制到 `plugin.path` 属性所指的路径中。`plugin.path` 属性包含在 Kafka Connect 集群 worker 节点配置文件中。
- Confluent cloud

  > **说明**
  >
  > Kafka connector 目前尚未上传到 Confluent Hub，您需要将其压缩包上传到 Confluent cloud。

### 创建 StarRocks 表

您需要根据 Kafka Topic 以及数据在 StarRocks 中创建对应的表。

## 使用示例

本文以自建 Kafka 集群为例，介绍如何配置 Kafka connector 并启动 Kafka Connect 服务（无需重启 Kafka 服务），导入数据至 StarRocks。

1. 创建 Kafka connector 配置文件 **connect-StarRocks-sink.properties**，并配置对应参数。参数和相关说明，参见[参数说明](#参数说明)。

    ```Properties
    name=starrocks-kafka-connector
    connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
    topics=dbserver1.inventory.customers
    starrocks.http.url=192.168.xxx.xxx:8030,192.168.xxx.xxx:8030
    starrocks.username=root
    starrocks.password=123456
    starrocks.database.name=inventory
    key.converter=io.confluent.connect.json.JsonSchemaConverter
    value.converter=io.confluent.connect.json.JsonSchemaConverter
    ```

    > **注意**
    >
    > 如果源端数据为 CDC 数据，例如 Debezium CDC 格式的数据，并且 StarRocks 表为主键表，为了将源端的数据变更同步至主键表，则您还需要[配置 `transforms` 以及相关参数](#导入-debezium-cdc-格式数据)。

2. 启动 Kafka Connect 服务（无需重启 Kafka 服务）。命令中的参数解释，参见 [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running)。

   1. Standalone 模式

      ```Shell
      bin/connect-standalone worker.properties connect-StarRocks-sink.properties [connector2.properties connector3.properties ...]
      ```

   2. 分布式模式

      > **说明**
      >
      > 生产环境中建议您使用分布式模式。

      1. 启动 worker：

          ```Shell
          bin/connect-distributed worker.properties
          ```

      2. 注意，分布式模式不支持启动时在命令行配置 Kafka connector。您需要通过调用 REST API 来配置 Kafka connector 和启动 Kafka connect:

        ```Shell
        curl -i http://127.0.0.1:8083/connectors -H "Content-Type: application/json" -X POST -d '{
          "name":"starrocks-kafka-connector",
          "config":{
            "connector.class":"com.starrocks.connector.kafka.SinkConnector",
            "topics":"dbserver1.inventory.customers",
            "starrocks.http.url":"192.168.xxx.xxx:8030,192.168.xxx.xxx:8030",
            "starrocks.user":"root",
            "starrocks.password":"123456",
            "starrocks.database.name":"inventory",
            "key.converter":"io.confluent.connect.json.JsonSchemaConverter",
            "value.converter":"io.confluent.connect.json.JsonSchemaConverter"
          }
        }
        ```

3. 查询 StarRocks 表中的数据。

## **参数说明**

| 参数                                | 是否必填 | 默认值                                                       | 描述                                                         |
| ----------------------------------- | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| name                                | 是       |                                                              | 表示当前的 Kafka connector，在 Kafka Connect 集群中必须为全局唯一，例如 starrocks-kafka-connector。 |
| connector.class                     | 是       | com.starrocks.connector.kafka.SinkConnector                  | kafka connector 的 sink 使用的类。                           |
| topics                              | 是       |                                                              | 一个或多个待订阅 Topic，每个 Topic 对应一个 StarRocks 表，默认情况下 Topic 的名称与 StarRocks 表名一致，导入时根据 Topic 名称确定目标 StarRocks 表。`topics` 和 `topics.regex`（如下） 两者二选一填写。如果两者不一致，则还需要配置 `starrocks.topic2table.map`。 |
| topics.regex                        |          | 与待订阅 Topic 匹配的正则表达式。更多解释，同 `topics`。`topics` 和 `topics.regex` 和 `topics`（如上）两者二选一填写。 |                                                              |
| starrocks.topic2table.map           | 否       |                                                              | 当 Topic 的名称与 StarRocks 表名不一致时，该配置项可以说明映射关系，格式为 `<topic-1>:<table-1>,<topic-2>:<table-2>,...`。 |
| starrocks.http.url                  | 是       |                                                              | FE 的 HTTP Server 地址。格式为 `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>,...`。多个地址，使用英文逗号 (,) 分隔。例如 `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`。 |
| starrocks.database.name             | 是       |                                                              | StarRocks 目标库名。                                         |
| starrocks.username                  | 是       |                                                              | StarRocks 用户名。用户需要具有目标表的 INSERT 权限。         |
| starrocks.password                  | 是       |                                                              | StarRocks 用户密码。                                         |
| key.converter                       | 否       | Kafka Connect 集群的 key converter                            | sink connector (在本场景中即 Kafka-connector-starrocks) 的 key converter，用于反序列化 Kafka 数据的 key。默认为 Kafka Connect 集群的 key converter, 您也可以自定义配置。      |
| value.converter                     | 否       | Kafka Connect 集群的 value converter                          | sink connector 的 value converter，用于反序列化 Kafka 数据的 value。默认为 Kafka Connect 集群的 value converter, 您也可以自定义配置。    |
| key.converter.schema.registry.url   | 否       |                                                              | key converter 对应的 schema registry 地址。                  |
| value.converter.schema.registry.url | 否       |                                                              | value converter 对应的 schema registry 地址。                |
| tasks.max                           | 否       | 1                                                            | Kafka connector 要创建的 task 线程数量上限，通常与 Kafka Connect 集群中的 worker 节点上的 CPU 核数量相同。如果需要增加导入性能的时候可以调整该参数。 |
| bufferflush.maxbytes                | 否       | 94371840(90M)                                                | 数据攒批的大小，达到该阈值后将数据通过 Stream Load 批量写入 StarRocks。取值范围：[64MB, 10GB]。 Stream Load SDK buffer可能会启动多个 Stream Load 来缓冲数据，因此这里的阈值是指总数据量大小。 |
| bufferflush.intervalms              | 否       | 300000                                                       | 数据攒批发送的间隔，用于控制数据写入 StarRocks 的延迟，取值范围：[1000, 3600000]。 |
| connect.timeoutms                   | 否       | 1000                                                         | 连接 http-url 的超时时间。取值范围：[100, 60000]。           |
| sink.properties.*                   |          |                                                              | 指定 Stream Load 的参数，用于控制导入行为，例如使用 `sink.properties.format` 指定导入数据的格式为 CSV 或者 JSON。更多参数和说明，请参见 [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。 |
| sink.properties.format              | 否       | json                                                         | Stream Load 导入时的数据格式。取值为 CSV 或者 JSON。默认为JSON。更多参数说明，参见 [CSV 适用参数](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#csv-适用参数)和 [JSON 适用参数](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#json-适用参数)。 |

## 使用限制

- 不支持将 Kafka topic 里的一条消息展开成多条导入到 StarRocks。
- Kafka Connector 的 Sink 保证 at-least-once 语义。

## 最佳实践

### 导入 Debezium CDC 格式数据

如果 Kafka 数据为 Debezium CDC 格式，并且 StarRocks 表为主键表，则在 Kafka connector 配置文件 **connect-StarRocks-sink.properties** 中除了[配置基础参数](#使用示例)外，还需要配置 `transforms` 以及相关参数。

```Properties
transforms=addfield,unwrap
transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=true
transforms.unwrap.delete.handling.mode=rewrite
```

在上述配置中，我们指定 `transforms=addfield,unwrap`。

- addfield transform 用于向 Debezium CDC 格式数据的每个记录添加一个__op字段，以支持 [主键表](../table_design/table_types/primary_key_table.md)，。如果 StarRocks 表不是主键表，则无需指定 addfield 转换。addfield transform 的类是 com.Starrocks.Kafka.Transforms.AddOpFieldForDebeziumRecord，已经包含在 Kafka connector 的 JAR 文件中，您无需手动安装。
- unwrap transform 是指由 Debezium 提供的 unwrap，可以根据操作类型 unwrap Debezium 复杂的数据结构。更多信息，参见 [New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)。
