---
displayed_sidebar: "English"
---

# Kafka connector

## Notifications

**User guide:** [Load data using Kafka connector](../loading/Kafka-connector-starrocks.md)

**Source codes:** [starrocks-connector-for-kafka](https://github.com/StarRocks/starrocks-connector-for-kafka)

**Naming format of the compressed file:** `starrocks-kafka-connector-${connector_version}.tar.gz`

**Download link of the compressed file:** [starrocks-kafka-connector](https://github.com/StarRocks/starrocks-connector-for-kafka/releases)

**Version requirements:**

| Kafka Connector | StarRocks | Java |
| --------------- | --------- | ---- |
| 1.0.0           | 2.1 and later | 8    |

## Release note

### 1.0

**1.0.0**

Release date: June 25, 2023

**Features**

- Supports loading CSV, JSON, Avro, and Protobuf data.
- Supports loading data from a self-managed Apache Kafka cluster or Confluent cloud.
