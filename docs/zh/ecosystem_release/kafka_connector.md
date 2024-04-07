---
displayed_sidebar: "Chinese"
---

# Kafka connector 版本发布

## 发布说明

**使用文档：** [使用 Kafka connector 导入数据](../loading/Kafka-connector-starrocks.md)

**源码下载地址：**[starrocks-connector-for-kafka](https://github.com/StarRocks/starrocks-connector-for-kafka)

**压缩包命名规则：**`starrocks-kafka-connector-${connector_version}.tar.gz`

**压缩包下载地址：**[starrocks-connector-for-kafka](https://github.com/StarRocks/starrocks-connector-for-kafka/releases)

**版本要求：**

| Kafka Connector  | StarRocks | Java |
| ---------------  | --------- | ---- |
| 1.0.0              | 2.1 及以上  | 8    |

## 发布记录

### 1.0

**1.0.0**

发布日期：2023 年 6 月 25 日

**新增功能**

- 支持导入 CSV、JSON、Avro 和 Protobuf 格式的数据。
- 支持从自建 Apache Kafka 集群或 Confluent cloud 导入数据。
