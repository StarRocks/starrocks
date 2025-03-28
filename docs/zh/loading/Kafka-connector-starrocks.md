---
displayed_sidebar: docs
---

# 使用 Kafka connector 导入数据

StarRocks 提供 Apache Kafka® 连接器 (StarRocks Connector for Apache Kafka®，简称 Kafka connector)，作为 sink connector，持续消费 Kafka 的消息并导入至 StarRocks 中。

使用 Kafka connector 可以更好的融入 Kafka 生态，StarRocks 可以与 Kafka Connect 无缝对接。为 StarRocks 准实时接入链路提供了更多的选择。相比于 Routine Load，您可以在以下场景中优先考虑使用 Kafka connector 导入数据：

- 相比于 Routine Load 仅支持导入 CSV、JSON、Avro 格式的数据，Kafka connector 支持导入更丰富的数据格式。只要数据能通过 Kafka Connect 的 converters 转换成 JSON 和 CSV 格式，就可以通过 Kafka connector 导入，例如 Protobuf 格式的数据。
- 需要对数据做自定义的 transform 操作，例如 Debezium CDC 格式的数据。
- 从多个 Kafka Topic 导入数据。
- 从 Confluent Cloud 导入数据。
- 需要更精细化的控制导入的批次大小，并行度等参数，以求达到导入速率和资源使用之间的平衡。

## 环境准备

### 版本要求

| Connector | Kafka | StarRocks | Java |
|-----------|-------|-----------| ---- |
| 1.0.4     | 3.4   | 2.5 及以上   | 8    |
| 1.0.3     | 3.4   | 2.5 及以上   | 8    |

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

    :::info

    - 在本示例中，StarRocks 提供的 Kafka connector 是 sink connector，能够持续消费 Kafka 的数据并导入 StarRocks。
    - 如果源端数据为 CDC 数据，例如 Debezium CDC 格式的数据，并且 StarRocks 表为主键表，为了将源端的数据变更同步至主键表，则您还需要在 StarRocks 提供的 Kafka connector 的配置文件 **connect-StarRocks-sink.properties** 中[配置 `transforms` 以及相关参数](#导入-debezium-cdc-格式数据)。

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

2. 配置并启动 Kafka Connect。

   1. 配置 Kafka Connect。在 **config** 目录中的 `config/connect-standalone.properties` 配置文件中配置如下参数。参数解释，参见 [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running)。

        ```yaml
        # kafka broker 的地址，多个 Broker 之间以英文逗号 (,) 分隔。
        # 注意本示例使用 PLAINTEXT 安全协议访问 Kafka 集群，如果使用其他安全协议访问 Kafka 集群，则您需要在本文件中配置相关信息。
        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # Kafka connector 解压后所在的绝对路径，例如：
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
        # Kafka connector 解压后所在的绝对路径，例如：
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```
  
   2. 启动 Kafka Connect。

        ```BASH
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-distributed.sh config/connect-distributed.properties
        ```

2. 配置并创建 Kafka connector。注意，在 Distributed 模式下您需要通过 REST API 来配置并创建 Kafka connector。参数和相关说明，参见[参数说明](#参数说明)。
  
    :::info

    - 在本示例中，StarRocks 提供的 Kafka connector 是 sink connector，能够持续消费 Kafka 的数据并导入 StarRocks。
    - 如果源端数据为 CDC 数据，例如 Debezium CDC 格式的数据，并且 StarRocks 表为主键表，为了将源端的数据变更同步至主键表，则您还需要在 StarRocks 提供的 Kafka connector 的配置文件 **connect-StarRocks-sink.properties** 中[配置 `transforms` 以及相关参数](#导入-debezium-cdc-格式数据)。

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
| connector.class                     | 是       |              | Kafka connector 的 sink 使用的类：com.starrocks.connector.kafka.StarRocksSinkConnector。                          |
| topics                              |       |                                                              | 一个或多个待订阅 Topic，每个 Topic 对应一个 StarRocks 表，默认情况下 Topic 的名称与 StarRocks 表名一致，导入时根据 Topic 名称确定目标 StarRocks 表。`topics` 和 `topics.regex`（如下） 两者二选一填写。如果两者不一致，则还需要配置 `starrocks.topic2table.map`。 |
| topics.regex                        |          | |与待订阅 Topic 匹配的正则表达式。更多解释，同 `topics`。`topics` 和 `topics.regex` 和 `topics`（如上）两者二选一填写。 |
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
| bufferflush.intervalms              | 否       | 1000                                                       | 数据攒批发送的间隔，用于控制数据写入 StarRocks 的延迟，取值范围：[1000, 3600000]。 |
| connect.timeoutms                   | 否       | 1000                                                         | 连接 http-url 的超时时间。取值范围：[100, 60000]。           |
| sink.properties.*                   |          |                                                              | 指定 Stream Load 的参数，用于控制导入行为，例如使用 `sink.properties.format` 指定导入数据的格式为 CSV 或者 JSON。更多参数和说明，请参见 [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)。 |
| sink.properties.format              | 否       | json                                                         | Stream Load 导入时的数据格式。取值为 CSV 或者 JSON。默认为JSON。更多参数说明，参见 [CSV 适用参数](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#csv-适用参数)和 [JSON 适用参数](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#json-适用参数)。 |
| sink.properties.partial_update      | 否      | `FALSE` | 是否使用部分更新。取值包括 `TRUE` 和 `FALSE`。默认值：`FALSE`。                                                                                                                                                                                             |
| sink.properties.partial_update_mode | 否      | `row` | 指定部分更新的模式，取值包括 `row` 和 `column`。<ul><li>`row`（默认值），指定使用行模式执行部分更新，比较适用于较多列且小批量的实时更新场景。</li><li>`column`，指定使用列模式执行部分更新，比较适用于少数列并且大量行的批处理更新场景。在该场景，开启列模式，更新速度更快。例如，在一个包含 100 列的表中，每次更新 10 列（占比 10%）并更新所有行，则开启列模式，更新性能将提高 10 倍。</li></ul>  |

## 使用说明

### Flush 策略

Kafka connector 会先在内存中缓存数据，然后通过 Stream Load 将其一次性落盘至 StarRocks。落盘将在以下任何条件满足时触发：

- 缓存的数据的字节达到限制 `bufferflush.maxbytes`。
- 自上次落盘以来经过的时间达到 connector 限制 `bufferflush.intervalms`。
- 达到了 Task 偏移量的提交间隔，由 Kafka Connect 配置项 [`offset.flush.interval.ms`](https://docs.confluent.io/platform/current/connect/references/allconfigs.html) 控制, 默认值是 `60000`。

如需降低数据延迟，可在 Kafka Connector 设置文件中调整以上配置。但请注意，频繁的 Flush 会增加 CPU 和 I/O 的使用。

### 使用限制

- 不支持将 Kafka topic 里的一条消息展开成多条导入到 StarRocks。
- StarRocks 提供的 Kafka connector 的 Sink 保证 at-least-once 语义。

## 最佳实践

### 导入 Debezium CDC 格式数据

Debezium 是一款 CDC 工具，可监控多种数据库的数据变更，并将变更流写入 Kafka。以下示例演示了如何配置和使用 Kafka Connector 将 PostgreSQL 的更改写入 StarRocks 中的**主键表**。

#### 步骤 1：安装启动 Kafka

> **说明**
>
> 如果您有自己的 Kafka 环境，可以跳过当前步骤。

1. [下载](https://dlcdn.apache.org/kafka/)最新版本的 Kafka，并解压压缩包。

   ```Bash
   tar -xzf kafka_2.13-3.7.0.tgz
   cd kafka_2.13-3.7.0
   ```

2. 启动 Kafka 环境。

   生成 Kafka 集群 UUID。

   ```Bash
   KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
   ```

   格式化日志目录。

   ```Bash
   bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
   ```

   启动 Kafka 服务。

   ```Bash
   bin/kafka-server-start.sh config/kraft/server.properties
   ```

#### 步骤 2：配置 PostgreSQL

1. 确保 PostgreSQL 用户拥有 `REPLICATION` 权限。

2. 修改 PostgreSQL 配置。

   在 **postgresql.conf** 文件中，将 `wal_level` 设置为 `logical`。

   ```Properties
   wal_level = logical
   ```

   重启 PostgreSQL 服务以使更改生效。

   ```Bash
   pg_ctl restart
   ```

3. 准备数据集。

   建表并插入测试数据。

   ```SQL
   CREATE TABLE customers (
     id int primary key ,
     first_name varchar(65533) NULL,
     last_name varchar(65533) NULL ,
     email varchar(65533) NULL 
   );

   INSERT INTO customers VALUES (1,'a','a','a@a.com');
   ```

4. 验证 Kafka 中的 CDC 日志消息。

    ```Json
    {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "int32",
                            "optional": false,
                            "field": "id"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "first_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "last_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "email"
                        }
                    ],
                    "optional": true,
                    "name": "test.public.customers.Value",
                    "field": "before"
                },
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "int32",
                            "optional": false,
                            "field": "id"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "first_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "last_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "email"
                        }
                    ],
                    "optional": true,
                    "name": "test.public.customers.Value",
                    "field": "after"
                },
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "string",
                            "optional": false,
                            "field": "version"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "connector"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "name"
                        },
                        {
                            "type": "int64",
                            "optional": false,
                            "field": "ts_ms"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "name": "io.debezium.data.Enum",
                            "version": 1,
                            "parameters": {
                                "allowed": "true,last,false,incremental"
                            },
                            "default": "false",
                            "field": "snapshot"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "db"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "sequence"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "schema"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "table"
                        },
                        {
                            "type": "int64",
                            "optional": true,
                            "field": "txId"
                        },
                        {
                            "type": "int64",
                            "optional": true,
                            "field": "lsn"
                        },
                        {
                            "type": "int64",
                            "optional": true,
                            "field": "xmin"
                        }
                    ],
                    "optional": false,
                    "name": "io.debezium.connector.postgresql.Source",
                    "field": "source"
                },
                {
                    "type": "string",
                    "optional": false,
                    "field": "op"
                },
                {
                    "type": "int64",
                    "optional": true,
                    "field": "ts_ms"
                },
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "string",
                            "optional": false,
                            "field": "id"
                        },
                        {
                            "type": "int64",
                            "optional": false,
                            "field": "total_order"
                        },
                        {
                            "type": "int64",
                            "optional": false,
                            "field": "data_collection_order"
                        }
                    ],
                    "optional": true,
                    "name": "event.block",
                    "version": 1,
                    "field": "transaction"
                }
            ],
            "optional": false,
            "name": "test.public.customers.Envelope",
            "version": 1
        },
        "payload": {
            "before": null,
            "after": {
                "id": 1,
                "first_name": "a",
                "last_name": "a",
                "email": "a@a.com"
            },
            "source": {
                "version": "2.5.3.Final",
                "connector": "postgresql",
                "name": "test",
                "ts_ms": 1714283798721,
                "snapshot": "false",
                "db": "postgres",
                "sequence": "[\"22910216\",\"22910504\"]",
                "schema": "public",
                "table": "customers",
                "txId": 756,
                "lsn": 22910504,
                "xmin": null
            },
            "op": "c",
            "ts_ms": 1714283798790,
            "transaction": null
        }
    }
    ```

#### 步骤 3：配置 StarRocks

在 StarRocks 中创建主键表，表结构需与 PostgreSQL 源表一致。

```SQL
CREATE TABLE `customers` (
  `id` int(11) COMMENT "",
  `first_name` varchar(65533) NULL COMMENT "",
  `last_name` varchar(65533) NULL COMMENT "",
  `email` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id`) 
DISTRIBUTED BY hash(id) buckets 1
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);
```

#### 步骤 4：安装 Connector

1. 下载 connector 解压到 **plugins** 目录。

   ```Bash
   mkdir plugins
   tar -zxvf debezium-debezium-connector-postgresql-2.5.3.zip -C plugins
   tar -zxvf starrocks-kafka-connector-1.0.3.tar.gz -C plugins
   ```

   该目录为 **config/connect-standalone.properties** 中设置项 `plugin.path` 的值。

   ```Properties
   plugin.path=/path/to/kafka_2.13-3.7.0/plugins
   ```

2. 在 **pg-source.properties** 中配置 PostgreSQL Source Connector。

   ```Json
   {
     "name": "inventory-connector",
     "config": {
       "connector.class": "io.debezium.connector.postgresql.PostgresConnector", 
       "plugin.name": "pgoutput",
       "database.hostname": "localhost", 
       "database.port": "5432", 
       "database.user": "postgres", 
       "database.password": "", 
       "database.dbname" : "postgres", 
       "topic.prefix": "test"
     }
   }
   ```

3. 在 **sr-sink.properties** 中配置 StarRocks Sink Connector。

   ```Json
   {
       "name": "starrocks-kafka-connector",
       "config": {
           "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
           "tasks.max": "1",
           "topics": "test.public.customers",
           "starrocks.http.url": "172.26.195.69:28030",
           "starrocks.database.name": "test",
           "starrocks.username": "root",
           "starrocks.password": "StarRocks@123",
           "sink.properties.strip_outer_array": "true",
           "connect.timeoutms": "3000",
           "starrocks.topic2table.map": "test.public.customers:customers",
           "transforms": "addfield,unwrap",
           "transforms.addfield.type": "com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord",
           "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
           "transforms.unwrap.drop.tombstones": "true",
           "transforms.unwrap.delete.handling.mode": "rewrite"
       }
   }
   ```

   > **说明**
   >
   > - 如果 StarRocks 表不是主键表，则无需指定 addfield transform。
   > - unwrap transform 是指由 Debezium 提供的 unwrap，可以根据操作类型 unwrap Debezium 复杂的数据结构。更多信息，参见 [New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)。

4. 配置 Kafka Connect。

   在 Kafka Connect 配置文件 **config/connect-standalone.properties** 中配置以下参数。

   ```Properties
   # Kafka Broker 的地址。多个 Kafka Broker 的地址需要用逗号（,）隔开。
   # 请注意，本示例使用 PLAINTEXT 作为访问 Kafka 集群的安全协议。
   # 如果使用其他安全协议访问 Kafka 集群，请在此处配置相关信息。

   bootstrap.servers=<kafka_broker_ip>:9092
   offset.storage.file.filename=/tmp/connect.offsets
   key.converter=org.apache.kafka.connect.json.JsonConverter
   value.converter=org.apache.kafka.connect.json.JsonConverter
   key.converter.schemas.enable=true
   value.converter.schemas.enable=false

   # 解压后 starrocks-kafka-connector 的绝对路径。示例：
   plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3

   # 控制 Flush 策略的参数。更多信息，请参阅使用说明部分。
   offset.flush.interval.ms=10000
   bufferflush.maxbytes = xxx
   bufferflush.intervalms = xxx
   ```

   更多配置详情请参阅 [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running)。

#### 步骤 5：以 Standalone 模式启动 Kafka Connect

运行 Kafka Connect Standalone 模式以启动 Connector。

```Bash
bin/connect-standalone.sh config/connect-standalone.properties config/pg-source.properties config/sr-sink.properties 
```

#### 步骤 6：验证数据导入

测试以下操作，确保数据正确导入到 StarRocks。

##### INSERT

- PostgreSQL:

```Plain
postgres=# insert into customers values (2,'b','b','b@b.com');
INSERT 0 1
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  1 | a          | a         | a@a.com
  2 | b          | b         | b@b.com
(2 rows)
```

- StarRocks:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    1 | a          | a         | a@a.com |
|    2 | b          | b         | b@b.com |
+------+------------+-----------+---------+
2 rows in set (0.01 sec)
```

##### UPDATE

- PostgreSQL:

```Plain
postgres=# update customers set email='c@c.com';
UPDATE 2
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  1 | a          | a         | c@c.com
  2 | b          | b         | c@c.com
(2 rows)
```

- StarRocks:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    1 | a          | a         | c@c.com |
|    2 | b          | b         | c@c.com |
+------+------------+-----------+---------+
2 rows in set (0.00 sec)
```

##### DELETE

- PostgreSQL:

```Plain
postgres=# delete from customers where id=1;
DELETE 1
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  2 | b          | b         | c@c.com
(1 row)
```

- StarRocks:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    2 | b          | b         | c@c.com |
+------+------------+-----------+---------+
1 row in set (0.00 sec)
```
