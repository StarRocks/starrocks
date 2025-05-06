---
displayed_sidebar: docs
---

# Kafka connector

## 通知

**ユーザーガイド:** [Load data using Kafka connector](../loading/Kafka-connector-starrocks.md)

**ソースコード:** [starrocks-connector-for-kafka](https://github.com/StarRocks/starrocks-connector-for-kafka)

**圧縮ファイルの命名形式:** `starrocks-kafka-connector-${connector_version}.tar.gz`

**圧縮ファイルのダウンロードリンク:** [starrocks-kafka-connector](https://github.com/StarRocks/starrocks-connector-for-kafka/releases)

**バージョン要件:**

| Kafka Connector | StarRocks | Java |
| --------------- | --------- | ---- |
| 1.0.0           | 2.1 and later | 8    |

## リリースノート

### 1.0

**1.0.0**

リリース日: 2023年6月25日

**機能**

- CSV、JSON、Avro、Protobuf データのロードをサポート。
- 自己管理型の Apache Kafka クラスターまたは Confluent クラウドからのデータロードをサポート。