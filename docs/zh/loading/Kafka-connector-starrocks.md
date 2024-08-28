---
displayed_sidebar: docs
---

# 使用 Kafka connector 导入数据

StarRocks 提供  Apache Kafka®  连接器 (StarRocks Connector for Apache Kafka®)，持续消费 Kafka 的消息并导入至 StarRocks 中。

使用 Kafka connector 可以更好的融入 Kafka 生态，StarRocks 可以与 Kafka Connect 无缝对接。为 StarRocks 准实时接入链路提供了更多的选择。相比于 Routine Load，您可以在以下场景中优先考虑使用 Kafka connector 导入数据：

- 相比于 Routine Load 仅支持导入 CSV、JSON、Avro 格式的数据，Kafka connector 支持导入更丰富的数据格式。只要数据能通过 Kafka Connect 的 converters 转换成 JSON 和 CSV 格式，就可以通过 Kafka connector 导入，例如 Protobuf 格式的数据。
- 需要对数据做自定义的 transform 操作，例如 Debezium CDC 格式的数据。
- 从多个 Kafka Topic 导入数据。
- 从 Confluent Cloud 导入数据。
- 需要更精细化的控制导入的批次大小，并行度等参数，以求达到导入速率和资源使用之间的平衡。

## 环境准备

### 准备 Kafka 环境

支持自建 Apache Kafka 集群和 Confluent Cloud：

- 如果使用自建 Apache Kafka 集群，您可以参考 [Apache Kafka quickstart](https://kafka.apache.org/quickstart) 快速部署 Kafka 集群。Kafka Connect 已集成在 Kafka 中。
- 如果使用 Confluent Cloud，请确保已拥有 Confluent 账号并已经创建集群。

### 下载 Kafka connector

安装 Kafka connector 至 Kafka connect。

- 自建 Kafka 集群

  下载并解压 [starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases)。

- Confluent Cloud

  Kafka connector 目前尚未上传到 Confluent Hub，您需要下载并解压 [starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases) ，打包成 ZIP 文件并上传到 Confluent Cloud。

### 网络配置

确保 Kafka 所在机器能够访问 StarRocks 集群中 FE 节点的 [`http_port`](../administration/management/FE_configuration.md#http_port)（默认 `8030`） 和 [`query_port`](../administration/management/FE_configuration.md#query_port) 端口（默认 `9030`），以及 BE 节点的 [`be_http_port`](../administration/management/BE_configuration.md#be_http_port) 端口（默认 `8040`）。

## 使用示例

本文以自建 Kafka 集群为例，介绍如何配置 Kafka connector 和 Kafka connect，然后启动 Kafka Connect 导入数据至 StarRocks。

### 数据集

假设 Kafka 集群的 Topic `test` 中存在如下 JSON 格式的数据。

```JSON
{"id":1,"city":"New York"}
{"id":2,"city":"Los Angeles"}
{"id":3,"city":"Chicago"}
```

### 目标数据库和表

根据 JSON 数据中需要导入的 key，在 StarRocks 集群的目标数据库 `example_db` 中创建表 `test_tbl` 。

```SQL
CREATE DATABASE example_db;
USE example_db;
CREATE TABLE test_tbl (id INT, city STRING);
```

### 配置 Kafka connector 和 Kafka Connect，然后启动 Kafka Connect 导入数据

#### 通过 Standalone 模式启动 Kafka Connect

1. 配置 Kafka connector。在 Kafka 安装目录下的 **config** 目录，创建 Kafka connector 的配置文件 **connect-StarRocks-sink.properties**，并配置对应参数。参数和相关说明，参见[参数说明](#参数说明)。

  :::note

  Kafka connector 是 sink connector。

  :::
      ```yaml
      name=starrocks-kafka-connector
      connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
      topics=test
      key.converter=org.apache.kafka.connect.json.JsonConverter
      value.converter=org.apache.kafka.connect.json.JsonConverter
      key.converter.schemas.enable=true
      value.converter.schemas.enable=false
      # StarRocks FE 的 HTTP Server 地址，默认端口 8030
      starrocks.http.url=192.168.xxx.xxx:8030
      # 当 Kafka Topic 的名称与 StarRocks 表名不一致时，需要配置两者的映射关系
      starrocks.topic2table.map=test:test_tbl
      # StarRocks 用户名
      starrocks.username=user1
      # StarRocks 用户密码。您必须输入用户密码。
      starrocks.password=123456
      starrocks.database.name=example_db
      sink.properties.strip_outer_array=true
      ```

    > **注意**
    >
    > 如果源端数据为 CDC 数据，例如 Debezium CDC 格式的数据，并且 StarRocks 表为主键表，为了将源端的数据变更同步至主键表，则您还需要[配置 `transforms` 以及相关参数](#导入-debezium-cdc-格式数据)。

2. 配置并启动 Kafka Connect。

   1. 配置 Kafka Connect。在 **config** 目录中的 `config/connect-standalone.properties` 配置文件中配置如下参数。参数解释，参见 [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running)。

        ```yaml
        # kafka broker 的地址，多个 Broker 之间以英文逗号 (,) 分隔。
        # 注意本示例使用 PLAINTEXT  安全协议访问 Kafka 集群，如果使用其他安全协议访问 Kafka 集群，则您需要在本文件中配置相关信息。
        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # 修改 starrocks-kafka-connector 为解压后的绝对路径，例如：
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```

   2. 启动 Kafka Connect。

        ```Bash
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-standalone.sh config/connect-standalone.properties config/connect-starrocks-sink.properties
        ```
#### 通过 Distributed 模式启动 Kafka Connect

1. 配置并启动 Kafka Connect。
   1. 配置 Kafka Connect。在 **config** 目录中的 `config/connect-distributed.properties` 配置文件中配置如下参数。参数解释，参见 [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running)。
        ```yaml
        # kafka broker 的地址，多个 Broker 之间以英文逗号 (,) 分隔。
        # 注意本示例使用 PLAINTEXT  安全协议访问 Kafka 集群，如果使用其他安全协议访问 Kafka 集群，则您需要在本文件中配置相关信息。        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # 修改 starrocks-kafka-connector 为解压后的绝对路径，例如：
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```
  
    2. 启动 Kafka Connect。

        ```BASH
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-distributed.sh config/connect-distributed.properties
        ```

2. 配置并创建 Kafka connector。注意，在 Distributed 模式下您需要通过 REST API 来配置并创建 Kafka connector。参数和相关说明，参见[参数说明](#参数说明)。
  
      :::note

      Kafka connector 是 sink connector。

      :::

      ```Shell
      curl -i http://127.0.0.1:8083/connectors -H "Content-Type: application/json" -X POST -d '{
        "name":"starrocks-kafka-connector",
        "config":{
          "connector.class":"com.starrocks.connector.kafka.StarRocksSinkConnector",
          "topics":"test",
          "key.converter":"org.apache.kafka.connect.json.JsonConverter",
          "value.converter":"org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable":"true",
          "value.converter.schemas.enable":"false",
          "starrocks.http.url":"192.168.xxx.xxx:8030",
          "starrocks.topic2table.map":"test:test_tbl",
          "starrocks.username":"user1",
          "starrocks.password":"123456",
          "starrocks.database.name":"example_db",
          "sink.properties.strip_outer_array":"true"
        }
      }'
      ```
    > **注意**
    >
    > 如果源端数据为 CDC 数据，例如 Debezium CDC 格式的数据，并且 StarRocks 表为主键表，为了将源端的数据变更同步至主键表，则您还需要[配置 `transforms` 以及相关参数](#导入-debezium-cdc-格式数据)。

#### 查询 StarRocks 表中的数据

查询 StarRocks 目标表 `test_tbl`，返回如下结果则表示数据已经成功导入。

```mysql
MySQL [example_db]> select * from test_tbl;
+------+-------------+
| id   | city        |
+------+-------------+
|    1 | New York    |
|    2 | Los Angeles |
|    3 | Chicago     |
+------+-------------+
3 rows in set (0.01 sec)
```

## 参数说明

| 参数                                | 是否必填 | 默认值                                                       | 描述                                                         |
| ----------------------------------- | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| name                                | 是       |                                                              | 表示当前的 Kafka connector，在 Kafka Connect 集群中必须为全局唯一，例如 starrocks-kafka-connector。 |
| connector.class                     | 是       |              | kafka connector 的 sink 使用的类：com.starrocks.connector.kafka.StarRocksSinkConnector。                          |
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
| sink.properties.*                   |          |                                                              | 指定 Stream Load 的参数，用于控制导入行为，例如使用 `sink.properties.format` 指定导入数据的格式为 CSV 或者 JSON。更多参数和说明，请参见 [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)。 |
| sink.properties.format              | 否       | json                                                         | Stream Load 导入时的数据格式。取值为 CSV 或者 JSON。默认为JSON。更多参数说明，参见 [CSV 适用参数](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#csv-适用参数)和 [JSON 适用参数](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#json-适用参数)。 |
| sink.properties.partial_update      | 否      | `FALSE` | 是否使用部分更新。取值包括 `TRUE` 和 `FALSE`。默认值：`FALSE`。                                                                                                                                                                                             |
| sink.properties.partial_update_mode | 否      | `row` | 指定部分更新的模式，取值包括 `row` 和 `column`。<ul><li>`row`（默认值），指定使用行模式执行部分更新，比较适用于较多列且小批量的实时更新场景。</li><li>`column`，指定使用列模式执行部分更新，比较适用于少数列并且大量行的批处理更新场景。在该场景，开启列模式，更新速度更快。例如，在一个包含 100 列的表中，每次更新 10 列（占比 10%）并更新所有行，则开启列模式，更新性能将提高 10 倍。</li></ul>  |

## 使用限制

- 不支持将 Kafka topic 里的一条消息展开成多条导入到 StarRocks。
- Kafka connector 的 Sink 保证 at-least-once 语义。

## 最佳实践

### 导入 Debezium CDC 格式数据

如果 Kafka 数据为 Debezium CDC 格式，并且 StarRocks 表为主键表，则在 Kafka connector 配置文件 **connect-StarRocks-sink.properties** 中除了[配置基础参数](#使用示例)外，还需要配置 `transforms` 以及相关参数。
  :::note

  Kafka connector 是 sink connector。

  :::

```Properties
transforms=addfield,unwrap
transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=true
transforms.unwrap.delete.handling.mode=rewrite
```

在上述配置中，我们指定 `transforms=addfield,unwrap`。

- 如果 StarRocks 表是主键表，则需要指定 addfield transform，用于向 Debezium CDC 格式数据的每个记录添加一个 `op` 字段。如果 StarRocks 表不是主键表，则无需指定 addfield transform。addfield transform 的类是 `com.Starrocks.Kafka.Transforms.AddOpFieldForDebeziumRecord`，已经包含在 Kafka connector 的 JAR 文件中，您无需手动安装。
- unwrap transform 是指由 Debezium 提供的 unwrap，可以根据操作类型 unwrap Debezium 复杂的数据结构。更多信息，参见 [New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)。
