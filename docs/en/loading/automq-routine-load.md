# Continuously load data from AutoMQ Kafka.md

<<<<<<< HEAD
[AutoMQ for Kafka](https://docs.automq.com/zh/docs/automq-s3kafka/YUzOwI7AgiNIgDk1GJAcu6Uanog) is a cloud-native version of Kafka redesigned for cloud environments.
=======
# AutoMQ Kafka

import Replicanum from '../assets/commonMarkdown/replicanum.md'

[AutoMQ for Kafka](https://docs.automq.com/docs/automq-s3kafka/YUzOwI7AgiNIgDk1GJAcu6Uanog) is a cloud-native version of Kafka redesigned for cloud environments.
>>>>>>> aa3898db51 ([Doc]delete-the-property-of-one-replica (#39422))
AutoMQ Kafka is [open source](https://github.com/AutoMQ/automq-for-kafka) and fully compatible with the Kafka protocol, fully leveraging cloud benefits.
Compared to self-managed Apache Kafka, AutoMQ Kafka, with its cloud-native architecture, offers features like capacity auto scaling, self-balancing of network traffic, move partition in seconds. These features contribute to a significantly lower Total Cost of Ownership (TCO) for users.

This article will guide you through importing data into AutoMQ Kafka using StarRocks Routine Load.
For an understanding of the basic principles of Routine Load, refer to the section on Routine Load Fundamentals.

## Prepare Environment

### Prepare StarRocks and test data

Ensure you have a running StarRocks cluster. For demonstration purposes, this article follow the [deployment guide](https://docs.starrocks.io/zh/docs/3.0/quick_start/deploy_with_docker/) to install a StarRocks cluster on a Linux machine via Docker.

Creating a database and test table with the primary key model:

```sql
create database automq_db;
create table users (
  id bigint NOT NULL,
  name string NOT NULL,
  timestamp string NULL,
  status string NULL
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
  "enable_persistent_index" = "true"
);
```

<Replicanum />

## Prepare AutoMQ Kafka and test data

To prepare your AutoMQ Kafka environment and test data, follow the AutoMQ [Quick Start](https://docs.automq.com/docs/automq-s3kafka/VKpxwOPvciZmjGkHk5hcTz43nde) guide to deploy your AutoMQ Kafka cluster. Ensure that StarRocks can directly connect to your AutoMQ Kafka server.

To quickly create a topic named `example_topic` in AutoMQ Kafka and write a test JSON data into it, follow these steps:

### Create a topic

Use Kafka’s command-line tools to create a topic. Ensure you have access to the Kafka environment and the Kafka service is running.
Here is the command to create a topic:

```shell
./kafka-topics.sh --create --topic example_topic --bootstrap-server 10.0.96.4:9092 --partitions 1 --replication-factor 1
```

> Note: Replace `topic` and `bootstrap-server` with your Kafka server address.

To check the result of the topic creation, use this command:

```shell
./kafka-topics.sh --describe example_topic --bootstrap-server 10.0.96.4:9092
```

### Generate test data

Generate a simple JSON format test data

```json
{
  "id": 1,
  "name": "testuser",
  "timestamp": "2023-11-10T12:00:00",
  "status": "active"
}
```

### Write Test Data

Use Kafka’s command-line tools or programming methods to write test data into example_topic. Here is an example using command-line tools:

```shell
echo '{"id": 1, "name": "testuser", "timestamp": "2023-11-10T12:00:00", "status": "active"}' | sh kafka-console-producer.sh --broker-list 10.0.96.4:9092 --topic example_topic
```

> Note: Replace `topic` and `bootstrap-server` with your Kafka server address.

To view the recently written topic data, use the following command:

```shell
sh kafka-console-consumer.sh --bootstrap-server 10.0.96.4:9092 --topic example_topic --from-beginning
```

## Creating a Routine Load Task

In the StarRocks command line, create a Routine Load task to continuously import data from the AutoMQ Kafka topic:

```sql
CREATE ROUTINE LOAD automq_example_load ON users
COLUMNS(id, name, timestamp, status)
PROPERTIES
(
  "desired_concurrent_number" = "5",
  "format" = "json",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.timestamp\",\"$.status\"]"
)
FROM KAFKA
(
  "kafka_broker_list" = "10.0.96.4:9092",
  "kafka_topic" = "example_topic",
  "kafka_partitions" = "0",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> Note: Replace `kafka_broker_list` with your Kafka server address.

### Explanation of Parameters

#### Data Format

Specify the data format as JSON in the "format" = "json" of the PROPERTIES clause.

#### Data Extraction and Transformation

To specify the mapping and transformation relationship between the source data and the target table, configure the COLUMNS and jsonpaths parameters. The column names in COLUMNS correspond to the column names of the target table, and their order corresponds to the column order in the source data. The jsonpaths parameter is used to extract the required field data from JSON data, similar to newly generated CSV data. Then the COLUMNS parameter temporarily names the fields in jsonpaths in order. For more explanations on data transformation, please see [Data Transformation during Import](https://docs.starrocks.io/zh/docs/3.0/loading/Etl_in_loading/).
> Note: If each JSON object per line has key names and quantities (order is not required) that correspond to the columns of the target table, there is no need to configure COLUMNS.

## Verifying Data Import

First, we check the Routine Load import job and confirm the Routine Load import task status is in RUNNING status.

```sql
show routine load\G;
```

Then, querying the corresponding table in the StarRocks database, we can observe that the data has been successfully imported.

```sql
StarRocks > select * from users;
+------+--------------+---------------------+--------+
| id   | name         | timestamp           | status |
+------+--------------+---------------------+--------+
|    1 | testuser     | 2023-11-10T12:00:00 | active |
|    2 | testuser     | 2023-11-10T12:00:00 | active |
+------+--------------+---------------------+--------+
2 rows in set (0.01 sec)
```
