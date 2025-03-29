---
displayed_sidebar: docs
---

# Load data using Kafka connector

StarRocks provides a self-developed connector named Apache Kafka® connector (StarRocks Connector for Apache Kafka®, Kafka connector for short), as a sink connector, that continuously consumes messages from Kafka and loads them into StarRocks. The Kafka connector guarantees at-least-once semantics.

The Kafka connector can seamlessly integrate with Kafka Connect, which allows StarRocks better integrated with the Kafka ecosystem. It is a wise choice if you want to load real-time data into StarRocks. Compared with Routine Load, it is recommended to use the Kafka connector in the following scenarios:

- Compared with Routine Load which only supports loading data in CSV, JSON, and Avro formats, Kafka connector can load data in more formats, such as Protobuf. As long as data can be converted into JSON and CSV formats using Kafka Connect's converters, data can be loaded into StarRocks via the Kafka connector.
- Customize data transformation, such as Debezium-formatted CDC data.
- Load data from multiple Kafka topics.
- Load data from Confluent Cloud.
- Need finer control over load batch sizes, parallelism, and other parameters to achieve a balance between load speed and resource utilization.

## Preparations

### Version requirements

| Connector | Kafka                    | StarRocks | Java |
|-----------|--------------------------|-----------| ---- |
| 1.0.4     | 3.4                      | 2.5 and later | 8    |
| 1.0.3     | 3.4                      | 2.5 and later | 8    |

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

    :::info

    - In this example, the Kafka connector provided by StarRocks is a sink connector that can continuously consume data from Kafka and load data into StarRocks.
    - If the source data is CDC data, such as data in Debezium format, and the StarRocks table is a Primary Key table, you also need to [configure `transform`](#load-debezium-formatted-cdc-data) in the configuration file **connect-StarRocks-sink.properties** for the Kafka connector provided by StarRocks, to synchronize the source data changes to the Primary Key table.

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
        # The absolute path of the Kafka connector after extraction. For example:
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
        # The absolute path of the Kafka connector after extraction. For example:
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```

    2. Run the Kafka Connect.

        ```BASH
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-distributed.sh config/connect-distributed.properties
        ```

2. Configure and create the Kafka connector. Note that in distributed mode, you need to configure and create the Kafka connector through the REST API. For parameters and descriptions, see [Parameters](#Parameters).

    :::info

    - In this example, the Kafka connector provided by StarRocks is a sink connector that can continuously consume data from Kafka and load data into StarRocks.
    - If the source data is CDC data, such as data in Debezium format, and the StarRocks table is a Primary Key table, you also need to [configure `transform`](#load-debezium-formatted-cdc-data) in the configuration file **connect-StarRocks-sink.properties** for the Kafka connector provided by StarRocks, to synchronize the source data changes to the Primary Key table.

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

**Required**:<br/>
**Default value**:<br/>
**Description**: One or more topics to subscribe to, where each topic corresponds to a StarRocks table. By default, StarRocks assumes that the topic name matches the name of the StarRocks table. So StarRocks determines the target StarRocks table by using the topic name. Please choose either to fill in `topics` or `topics.regex` (below), but not both. However, if the StarRocks table name is not the same as the topic name, then use the optional `starrocks.topic2table.map` parameter (below) to specify the mapping from topic name to table name.

### topics.regex

**Required**:<br/>
**Default value**:
**Description**: Regular expression to match the one or more topics to subscribe to. For more description, see `topics`. Please choose either to fill in `topics.regex` or `topics` (above), but not both. <br/>

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
**Default value**: 1000<br/>
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

## Usage Notes

### Flush Policy

The Kafka connector will buffer the data in memory, and flush them in batch to StarRocks via Stream Load. The flush will be triggered when any of the following conditions are met:

- The bytes of buffered rows reaches the limit `bufferflush.maxbytes`.
- The elapsed time since the last flush reaches the limit `bufferflush.intervalms`.
- The interval at which the connector tries committing offsets for tasks is reached. The interval is controlled by the Kafka Connect configuration [`offset.flush.interval.ms`](https://docs.confluent.io/platform/current/connect/references/allconfigs.html), and the default values is `60000`.

For lower data latency, adjust these configurations in the Kafka connector settings. However, more frequent flushes will increase CPU and I/O usage.

### Limits

- It is not supported to flatten a single message from a Kafka topic into multiple data rows and load into StarRocks.
- The sink of the Kafka connector provided by StarRocks guarantees at-least-once semantics.

## Best practices

### Load Debezium-formatted CDC data

Debezium is a popular Change Data Capture (CDC) tool that supports monitoring data changes in various database systems and streaming these changes to Kafka. The following example demonstrates how to configure and use the Kafka connector to write PostgreSQL changes to a **Primary Key table** in StarRocks.

#### Step 1: Install and start Kafka

> **NOTE**
>
> You can skip this step if you have your own Kafka environment.

1. [Download](https://dlcdn.apache.org/kafka/) the latest Kafka release from the official site and extract the package.

   ```Bash
   tar -xzf kafka_2.13-3.7.0.tgz
   cd kafka_2.13-3.7.0
   ```

2. Start the Kafka environment.

   Generate a Kafka cluster UUID.

   ```Bash
   KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
   ```

   Format the log directories.

   ```Bash
   bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
   ```

   Start the Kafka server.

   ```Bash
   bin/kafka-server-start.sh config/kraft/server.properties
   ```

#### Step 2: Configure PostgreSQL

1. Make sure the PostgreSQL user is granted `REPLICATION` privileges.

2. Adjust PostgreSQL configuration.

   Set `wal_level` to `logical` in **postgresql.conf**.

   ```Properties
   wal_level = logical
   ```

   Restart the PostgreSQL server to apply changes.

   ```Bash
   pg_ctl restart
   ```

3. Prepare the dataset.

   Create a table and insert test data.

   ```SQL
   CREATE TABLE customers (
     id int primary key ,
     first_name varchar(65533) NULL,
     last_name varchar(65533) NULL ,
     email varchar(65533) NULL 
   );

   INSERT INTO customers VALUES (1,'a','a','a@a.com');
   ```

4. Verify the CDC log messages in Kafka.

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

#### Step 3: Configure StarRocks

Create a Primary Key table in StarRocks with the same schema as the source table in PostgreSQL.

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

#### Step 4: Install connector

1. Download the connectors and extract the packages in the **plugins** directory.

   ```Bash
   mkdir plugins
   tar -zxvf debezium-debezium-connector-postgresql-2.5.3.zip -C plugins
   tar -zxvf starrocks-kafka-connector-1.0.3.tar.gz -C plugins
   ```

   This directory is the value of the configuration item `plugin.path` in **config/connect-standalone.properties**.

   ```Properties
   plugin.path=/path/to/kafka_2.13-3.7.0/plugins
   ```

2. Configure the PostgreSQL source connector in **pg-source.properties**.

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

3. Configure the StarRocks sink connector in **sr-sink.properties**.

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

   > **NOTE**
   >
   > - If the StarRocks table is not a Primary Key table, you do not need to specify the `addfield` transform.
   > - The unwrap transform is provided by Debezium and is used to unwrap Debezium's complex data structure based on the operation type. For more information, see [New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html).

4. Configure Kafka Connect.

   Configure the following configuration items in the Kafka Connect configuration file **config/connect-standalone.properties**.

   ```Properties
   # The addresses of Kafka brokers. Multiple addresses of Kafka brokers need to be separated by commas (,).
   # Note that this example uses PLAINTEXT as the security protocol to access the Kafka cluster.
   # If you use other security protocol to access the Kafka cluster, configure the relevant information in this part.

   bootstrap.servers=<kafka_broker_ip>:9092
   offset.storage.file.filename=/tmp/connect.offsets
   key.converter=org.apache.kafka.connect.json.JsonConverter
   value.converter=org.apache.kafka.connect.json.JsonConverter
   key.converter.schemas.enable=true
   value.converter.schemas.enable=false

   # The absolute path of the starrocks-kafka-connector after extraction. For example:
   plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3

   # Parameters that control the flush policy. For more information, see the Usage Note section.
   offset.flush.interval.ms=10000
   bufferflush.maxbytes = xxx
   bufferflush.intervalms = xxx
   ```

   For descriptions of more parameters, see [Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running).

#### Step 5: Start Kafka Connect in Standalone Mode

Run Kafka Connect in standalone mode to initiate the connectors.

```Bash
bin/connect-standalone.sh config/connect-standalone.properties config/pg-source.properties config/sr-sink.properties 
```

#### Step 6: Verify data ingestion

Test the following operations and ensure the data is correctly ingested into StarRocks.

##### INSERT

- In PostgreSQL:

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

- In StarRocks:

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

- In PostgreSQL:

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

- In StarRocks:

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

- In PostgreSQL:

```Plain
postgres=# delete from customers where id=1;
DELETE 1
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  2 | b          | b         | c@c.com
(1 row)
```

- In StarRocks:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    2 | b          | b         | c@c.com |
+------+------------+-----------+---------+
1 row in set (0.00 sec)
```
