---
displayed_sidebar: docs
---

# Load data using Kafka connector

StarRocks provides a self-developed connector named Apache Kafka® connector (StarRocks Connector for Apache Kafka®) that continuously consumes messages from Kafka and loads them into StarRocks. The Kafka connector guarantees at-least-once semantics.

The Kafka connector can seamlessly integrate with Kafka Connect, which allows StarRocks better integrated with the Kafka ecosystem. It is a wise choice if you want to load real-time data into StarRocks. Compared with Routine Load, it is recommended to use the Kafka connector in the following scenarios:

- Compared with Routine Load which only supports loading data in CSV, JSON, and Avro formats, Kafka connector can load data in more formats, such as Protobuf. As long as data can be converted into JSON and CSV formats using Kafka Connect's converters, data can be loaded into StarRocks via the Kafka connector.
- Customize data transformation, such as Debezium-formatted CDC data.
- Load data from multiple Kafka topics.
- Load data from Confluent Cloud.
- Need finer control over load batch sizes, parallelism, and other parameters to achieve a balance between load speed and resource utilization.

## Preparations

### Set up Kafka environment

Both self-managed Apache Kafka clusters and Confluent Cloud are supported.

- For a self-managed Apache Kafka cluster, you can refer to [Apache Kafka quickstart](https://kafka.apache.org/quickstart) to quickly deploy a Kafka cluster. Kafka Connect is already integrated into Kafka.
- For Confluent Cloud, make sure that you have a Confluent account and have created a cluster.

### Download Kafka connector

Submit the Kafka connector into Kafka Connect:

- Self-managed Kafka cluster:

  Download and extract [starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases).

- Confluent Cloud:

  Currently, the Kafka connector is not uploaded to Confluent Hub. You need to download and extract [starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases), package it into a ZIP file and upload the ZIP file to Confluent Cloud.

### Network configuration

Ensure that the machine where Kafka is located can access the FE nodes of the StarRocks cluster via the [`http_port`](../administration/management/FE_configuration.md#http_port) (default: `8030`) and [`query_port`](../administration/management/FE_configuration.md#query_port) (default: `9030`), and the BE nodes via the [`be_http_port`](../administration/management/BE_configuration.md#be_http_port) (default: `8040`).

## Usage

This section uses a self-managed Kafka cluster as an example to explain how to configure the Kafka connector and the Kafka Connect, and then run the Kafka Connect to load data into StarRocks.

### Prepare a dataset

Suppose that JSON-format data exists in the topic `test` in a Kafka cluster.

```JSON
{"id":1,"city":"New York"}
{"id":2,"city":"Los Angeles"}
{"id":3,"city":"Chicago"}
```

### Create a table

Create the table `test_tbl` in the database `example_db` in the StarRocks cluster according to the keys of the JSON-format data.

```SQL
CREATE DATABASE example_db;
USE example_db;
CREATE TABLE test_tbl (id INT, city STRING);
```

### Configure Kafka connector and Kafka Connect, and then run Kafka Connect to load data

#### Run Kafka Connect in standalone mode

1. Configure the Kafka connector. In the **config** directory under the Kafka installation directory, create the configuration file **connect-StarRocks-sink.properties** for the Kafka connector, and configure the following parameters. For more parameters and descriptions, see [Parameters](#Parameters).

    :::note

    The Kafka connector is a sink connector.

    :::

    ```yaml
    name=starrocks-kafka-connector
    connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
    topics=test
    key.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable=true
    value.converter.schemas.enable=false
    # The HTTP URL of the FE in your StarRocks cluster. The default port is 8030.
    starrocks.http.url=192.168.xxx.xxx:8030
    # If the Kafka topic name is different from the StarRocks table name, you need to configure the mapping relationship between them.
    starrocks.topic2table.map=test:test_tbl
    # Enter the StarRocks username.
    starrocks.username=user1
    # Enter the StarRocks password.
    starrocks.password=123456
    starrocks.database.name=example_db
    sink.properties.strip_outer_array=true
    ```

    > **NOTICE**
    >
    > If the source data is CDC data, such as data in Debezium format, and the StarRocks table is a Primary Key table, you also need to [configure `transform`](#load-debezium-formatted-cdc-data) in order to synchronize the source data changes to the Primary Key table.

2. Configure and run the Kafka Connect.

   1. Configure the Kafka Connect. In the configuration file **config/connect-standalone.properties** in the **config** directory, configure the following parameters. For more parameters and descriptions, see [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running).

        ```yaml
        # The addresses of Kafka brokers. Multiple addresses of Kafka brokers need to be separated by commas (,).
        # Note that this example uses PLAINTEXT as the security protocol to access the Kafka cluster. If you are using other security protocol to access the Kafka cluster, you need to configure the relevant information in this file.
        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # The absolute path of the starrocks-kafka-connector after extraction. For example:
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```

   2. Run the Kafka Connect.

        ```Bash
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-standalone.sh config/connect-standalone.properties config/connect-starrocks-sink.properties
        ```

#### Run Kafka Connect in distributed mode

1. Configure and run the Kafka Connect.

    1. Configure the Kafka Connect. In the configuration file `config/connect-distributed.properties` in the **config** directory, configure the following parameters. For more parameters and descriptions, refer to [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running).

        ```yaml
        # The addresses of Kafka brokers. Multiple addresses of Kafka brokers need to be separated by commas (,).
        # Note that this example uses PLAINTEXT as the security protocol to access the Kafka cluster. If you are using other security protocol to access the Kafka cluster, you need to configure the relevant information in this file.
        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # The absolute path of the starrocks-kafka-connector after extraction. For example:
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```

    2. Run the Kafka Connect.

        ```BASH
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-distributed.sh config/connect-distributed.properties
        ```

2. Configure and create the Kafka connector. Note that in distributed mode, you need to configure and create the Kafka connector through the REST API. For parameters and descriptions, see [Parameters](#Parameters).

      :::note

      The Kafka connector is a sink connector.

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

      :::info
      
      If the source data is CDC data, such as data in Debezium format, and the StarRocks table is a Primary Key table, you also need to [configure `transform`](#load-debezium-formatted-cdc-data) in order to synchronize the source data changes to the Primary Key table.

      :::

#### Query StarRocks table

Query the target StarRocks table `test_tbl`.

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

The data is successfully loaded when the above result is returned.

## Parameters

### name

**Required**: YES<br/>
**Default value**:<br/>
**Description**: Name for this Kafka connector. It must be globally unique among all Kafka connectors within this Kafka Connect cluster. For example, starrocks-kafka-connector.

### connector.class

**Required**: YES<br/>
**Default value**: <br/>
**Description**: Class used by this Kafka connector's sink. Set the value to `com.starrocks.connector.kafka.StarRocksSinkConnector`.

### topics

**Required**: YES<br/>
**Default value**:<br/>
**Description**: One or more topics to subscribe to, where each topic corresponds to a StarRocks table. By default, StarRocks assumes that the topic name matches the name of the StarRocks table. So StarRocks determines the target StarRocks table by using the topic name. Please choose either to fill in `topics` or `topics.regex` (below), but not both.However, if the StarRocks table name is not the same as the topic name, then use the optional `starrocks.topic2table.map` parameter (below) to specify the mapping from topic name to table name.

### topics.regex

**Required**:<br/>
**Default value**: Regular expression to match the one or more topics to subscribe to. For more description, see `topics`. Please choose either to fill in  `topics.regex`or `topics` (above), but not both. <br/>
**Description**:

### starrocks.topic2table.map

**Required**: NO<br/>
**Default value**:<br/>
**Description**: The mapping of the StarRocks table name and the topic name when the topic name is different from the StarRocks table name. The format is `<topic-1>:<table-1>,<topic-2>:<table-2>,...`.

### starrocks.http.url

**Required**: YES<br/>
**Default value**:<br/>
**Description**: The HTTP URL of the FE in your StarRocks cluster. The format is `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>,...`. Multiple addresses are separated by commas (,). For example, `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`.

### starrocks.database.name

**Required**: YES<br/>
**Default value**:<br/>
**Description**: The name of StarRocks database.

### starrocks.username

**Required**: YES<br/>
**Default value**:<br/>
**Description**: The username of your StarRocks cluster account. The user needs the [INSERT](../sql-reference/sql-statements/account-management/GRANT.md) privilege on the StarRocks table.

### starrocks.password

**Required**: YES<br/>
**Default value**:<br/>
**Description**: The password of your StarRocks cluster account.

### key.converter

**Required**: NO<br/>
**Default value**: Key converter used by Kafka Connect cluster<br/>
**Description**: This parameter specifies the key converter for the sink connector (Kafka-connector-starrocks), which is used to deserialize the keys of Kafka data. The default key converter is the one used by Kafka Connect cluster.

### value.converter

**Required**: NO<br/>
**Default value**: Value converter used by Kafka Connect cluster<br/>
**Description**: This parameter specifies the value converter for the sink connector (Kafka-connector-starrocks), which is used to deserialize the values of Kafka data. The default value converter is the one used by Kafka Connect cluster.

### key.converter.schema.registry.url

**Required**: NO<br/>
**Default value**:<br/>
**Description**: Schema registry URL for the key converter.

### value.converter.schema.registry.url

**Required**: NO<br/>
**Default value**:<br/>
**Description**: Schema registry URL for the value converter.

### tasks.max

**Required**: NO<br/>
**Default value**: 1<br/>
**Description**: The upper limit for the number of task threads that the Kafka connector can create, which is usually the same as the number of CPU cores on the worker nodes in the Kafka Connect cluster. You can tune this parameter to control load performance.

### bufferflush.maxbytes

**Required**: NO<br/>
**Default value**: 94371840(90M)<br/>
**Description**: The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. The maximum value ranges from 64 MB to 10 GB. Keep in mind that the Stream Load SDK buffer may create multiple Stream Load jobs to buffer data. Therefore, the threshold mentioned here refers to the total data size.

### bufferflush.intervalms              

**Required**: NO<br/>
**Default value**: 300000<br/>
**Description**: Interval for sending a batch of data which controls the load latency. Range: [1000, 3600000].

### connect.timeoutms                   

**Required**: NO<br/>
**Default value**: 1000<br/>
**Description**: Timeout for connecting to the HTTP URL. Range: [100, 60000].

### sink.properties.*                   

**Required**:<br/>
**Default value**:<br/>
**Description**: Stream Load parameters o control load behavior. For example, the parameter `sink.properties.format` specifies the format used for Stream Load, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md).

### sink.properties.format              

**Required**: NO<br/>
**Default value**: json<br/>
**Description**: The format used for Stream Load. The Kafka connector will transform each batch of data to the format before sending them to StarRocks. Valid values: `csv` and `json`. For more information, see [CSV parameters](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#csv-parameters) and [JSON parameters](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#json-parameters).

### sink.properties.partial_update

**Required**:  NO<br/>
**Default value**: `FALSE`<br/>
**Description**: Whether to use partial updates. Valid values: `TRUE` and `FALSE`. Default value: `FALSE`, indicating to disable this feature.

### sink.properties.partial_update_mode

**Required**:  NO<br/>
**Default value**: `row`<br/>
**Description**: Specifies the mode for partial updates. Valid values: `row` and `column`. <ul><li> The value `row` (default) means partial updates in row mode, which is more suitable for real-time updates with many columns and small batches.</li><li>The value `column` means partial updates in column mode, which is more suitable for batch updates with few columns and many rows. In such scenarios, enabling the column mode offers faster update speeds. For example, in a table with 100 columns, if only 10 columns (10% of the total) are updated for all rows, the update speed of the column mode is 10 times faster.</li></ul>

## Limits

- It is not supported to flatten a single message from a Kafka topic into multiple data rows and load into StarRocks.
- The Kafka connector's sink guarantees at-least-once semantics.

## Best practices

### Load Debezium-formatted CDC data

If the Kafka data is in Debezium CDC format and the StarRocks table is a Primary Key table, you also need to configure the `transforms` parameter and other related parameters.

  :::note

  The Kafka connector is a sink connector.

  :::

```Properties
transforms=addfield,unwrap
transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=true
transforms.unwrap.delete.handling.mode
```

In the above configurations, we specify `transforms=addfield,unwrap`.

- If the StarRocks table is a Primary Key table, you need to specify the addfield transform to add an `op` field to each record of the Debezium CDC formatted data. If the StarRocks table is not a Primary Key table, you do not need to specify the addfield transform. The addfield transform class is `com.Starrocks.Kafka.Transforms.AddOpFieldForDebeziumRecord`. It is included in the Kafka connector JAR file, so you do not need to manually install it.
- The unwrap transform is provided by Debezium and is used to unwrap Debezium's complex data structure based on the operation type. For more information, see [New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html).
