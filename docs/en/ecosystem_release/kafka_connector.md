---
displayed_sidebar: "English"
---

# Releases of StarRocks Connector for Kafka

## Notifications

**User guide:** [Load data using Kafka connector](../loading/Kafka-connector-starrocks.md)

**Source codes:** [starrocks-connector-for-kafka](https://github.com/StarRocks/starrocks-connector-for-kafka)

**Naming format of the compressed file:** `starrocks-kafka-connector-${connector_version}.tar.gz`

**Download link of the compressed file:** [starrocks-kafka-connector](https://github.com/StarRocks/starrocks-connector-for-kafka/releases)

**Version requirements:**

| Kafka Connector | StarRocks | Java |
| --------------- | --------- | ---- |
| 1.0.3           | 2.1 and later | 8    |
| 1.0.2           | 2.1 and later | 8    |
| 1.0.1           | 2.1 and later | 8    |
| 1.0.0           | 2.1 and later | 8    |

## Release notes

### 1.0

#### 1.0.3

Release date: December 19, 2023

**Features**

Added Apache License as the open-source software license. [#9](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/9)

#### 1.0.2

Release date: December 14, 2023

**Features**

Supports the source data to be of DECIMAL type. [#7](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/7)

#### 1.0.1

Release date: November 28, 2023

**Features**

- Supports loading Debezium data into Primary Key tables. [#4](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/4)
- Supports parsing JSON data without schema registry. [#6](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/6)

#### 1.0.0

Release date: June 25, 2023

**Features**

- Supports loading CSV, JSON, Avro, and Protobuf data.
- Supports loading data from a self-managed Apache Kafka cluster or Confluent cloud.
