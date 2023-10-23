# Load data using Kafka connector

StarRocks provides a self-developed connector named Apache Kafka® connector (StarRocks Connector for Apache Kafka®) that continuously consumes messages from Kafka and loads them into StarRocks. The Kafka connector guarantees at-least-once semantics.

The Kafka connector can seamlessly integrate with Kafka Connect, which allows StarRocks better integrated with the Kafka ecosystem. It is a wise choice if you want to load real-time data into StarRocks. Compared with Routine Load, it is recommended to use the Kafka connector in the following scenarios:

- The format of source data is, for example, Protobuf, not JSON, CSV, or Avro.
- Customize data transformation, such as Debezium-formatted CDC data.
- Load data from multiple Kafka topics.
- Load data from Confluent cloud.
- Need finer control over load batch sizes, parallelism, and other parameters to achieve a balance between load speed and resource utilization.

## Preparations

### Set up Kafka environment

Both self-managed Apache Kafka clusters and Confluent cloud are supported.

- For a self-managed Apache Kafka cluster, make sure that you deploy the Apache Kafka cluster and Kafka Connect cluster and create topics.
- For Confluent cloud, make sure that you have a Confluent account and create clusters and topics.

### Install Kafka connector

Submit the Kafka connector into Kafka Connect:

- Self-managed Kafka cluster:

  - Download and unzip [starrocks-kafka-connector-1.0.0.tar.gz](https://releases.starrocks.io/starrocks/starrocks-kafka-connector-1.0.0.tar.gz).
  - Copy the extracted directory to the `<kafka_dir>/libs` directory. Add the directory path containing the JAR files into the `plugin.path` worker configuration property in the Kafka connect cluster.

- Confluent cloud:

  > **NOTE**
  >
  > The Kafka connector is not currently uploaded to Confluent Hub. You need to upload the compressed file to Confluent cloud.

### Create StarRocks table

Create a table or tables in StarRocks according to Kafka Topics and data.

## Examples

The following steps take a self-managed Kafka cluster as an example to demonstrate how to configure the Kafka connector and start the Kafka connect (no need to restart the Kafka service) in order to load data into StarRocks.

1. Create a Kafka connector configuration file named **connect-StarRocks-sink.properties** and configure the  parameters. For detailed information about parameters, see [Parameters](#parameters).

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

    > **NOTICE**
    >
    > If the source data is CDC data, such as data in Debezium format, and the StarRocks table is a Primary Key table, you also need to [configure `transform`](#load-debezium-formatted-cdc-data) in order to synchronize the source data changes to the Primary Key table.

2. Run the Kafka Connector (no need to restart the Kafka service). For parameters and description in the following command, see [Kafka Documentation](https://kafka.apache.org/documentation.html#connect_running).

    - Standalone mode

        ```shell
        bin/connect-standalone worker.properties connect-StarRocks-sink.properties [connector2.properties connector3.properties ...]
        ```

    - Distributed mode

      > **NOTE**
      >
      > It is recommended to use the distributed mode in the production environment.

    - Start the worker.

        ```shell
        bin/connect-distributed worker.properties
        ```

    - Note that in distributed mode the connector configurations are not passed on the command line. Instead, use the REST API described below to configure the Kafka connector and run the Kafka connect.

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

3. Query data in the StarRocks table.

## Parameters

| Parameter                           | Required | Default value                                                | Description                                                  |
| ----------------------------------- | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| name                                | YES      |                                                              | Name for this Kafka connector. It must be globally unique among all Kafka connectors within this Kafka connect cluster. For example, starrocks-kafka-connector. |
| connector.class                     | YES      | com.starrocks.connector.kafka.SinkConnector                  | Class used by this Kafka connector's sink.                   |
| topics                              | YES      |                                                              | One or more topics to subscribe to, where each topic corresponds to a StarRocks table. By default, StarRocks assumes that the topic name matches the name of the StarRocks table. So StarRocks determines the target StarRocks table by using the topic name. Please choose either to fill in `topics` or `topics.regex` (below), but not both.However, if the StarRocks table name is not the same as the topic name, then use the optional `starrocks.topic2table.map` parameter (below) to specify the mapping from topic name to table name. |
| topics.regex                        |          | Regular expression to match the one or more topics to subscribe to. For more description, see `topics`. Please choose either to fill in  `topics.regex`or `topics` (above), but not both. |                                                              |
| starrocks.topic2table.map           | NO       |                                                              | The mapping of the StarRocks table name and the topic name when the topic name is different from the StarRocks table name. The format is `<topic-1>:<table-1>,<topic-2>:<table-2>,...`. |
| starrocks.http.url                  | YES      |                                                              | The HTTP URL of the FE in your StarRocks cluster. The format is `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>,...`. Multiple addresses are separated by commas (,). For example, `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`. |
| starrocks.database.name             | YES      |                                                              | The name of StarRocks database.                              |
| starrocks.username                  | YES      |                                                      | The username of your StarRocks cluster account. The user needs the [INSERT](../sql-reference/sql-statements/account-management/GRANT.md) privilege on the StarRocks table. |
| starrocks.password                  | YES      |                                                              | The password of your StarRocks cluster account.              |
| key.converter                       | NO      |           Key converter used by Kafka connect cluster                                                           | This parameter specifies the key converter for the sink connector (Kafka-connector-starrocks), which is used to deserialize the keys of Kafka data. The default key converter is the one used by Kafka connect cluster.|
| value.converter                     | NO      |    Value converter used by Kafka connect cluster                             | This parameter specifies the value converter for the sink connector (Kafka-connector-starrocks), which is used to deserialize the values of Kafka data. The default value converter is the one used by Kafka connect cluster. |
| key.converter.schema.registry.url   | NO       |                                                              | Schema registry URL for the key converter.                   |
| value.converter.schema.registry.url | NO       |                                                              | Schema registry URL for the value converter.                 |
| tasks.max                           | NO       | 1                                                            | The upper limit for the number of task threads that the Kafka connector can create, which is usually the same as the number of CPU cores on the worker nodes in the Kafka Connect cluster. You can tune this parameter to control load performance. |
| bufferflush.maxbytes                | NO       | 94371840(90M)                                                | The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. The maximum value ranges from 64 MB to 10 GB. Keep in mind that the Stream Load SDK buffer may create multiple Stream Load jobs to buffer data. Therefore, the threshold mentioned here refers to the total data size. |
| bufferflush.intervalms              | NO       | 300000                                                       | Interval for sending a batch of data which controls the load latency. Range: [1000, 3600000]. |
| connect.timeoutms                   | NO       | 1000                                                         | Timeout for connecting to the HTTP URL. Range: [100, 60000]. |
| sink.properties.*                   |          |                                                              | Stream Load parameters o control load behavior. For example, the parameter `sink.properties.format` specifies the format used for Stream Load, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM LOAD.md). |
| sink.properties.format              | NO       | json                                                         | The format used for Stream Load. The Kafka connector will transform each batch of data to the format before sending them to StarRocks. Valid values: `csv` and `json`. For more information, see [CSV parameters**](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#csv-parameters)和** [JSON parameters](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#json-parameters). |

## Limits

- It is not supported to flatten a single message from a Kafka topic into multiple data rows and load into StarRocks.
- The Kafka Connector's Sink guarantees at-least-once semantics.

## Best practices

### Load Debezium-formatted CDC data

If the Kafka data is in Debezium CDC format and the StarRocks table is a Primary Key table, you also need to configure the `transforms` parameter and other related parameters.

```Properties
transforms=addfield,unwrap
transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=true
transforms.unwrap.delete.handling.mode
```

In the above configurations, we specify `transforms=addfield,unwrap`.

- The addfield transform is used to add the __op field to each record of Debezium CDC-formatted data to support the StarRocks primary key model table. If the StarRocks table is not a Primary Key table, you do not need to specify the addfield transform. The addfield transform class is com.Starrocks.Kafka.Transforms.AddOpFieldForDebeziumRecord. It is included in the Kafka connector JAR file, so you do not need to manually install it.
- The unwrap transform is provided by Debezium and is used to unwrap Debezium's complex data structure based on the operation type. For more information, see [New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html).
