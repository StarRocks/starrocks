---
displayed_sidebar: docs
---

# Releases of StarRocks Connector for Kafka

## Notifications

**User guide:** [Load data using Kafka connector](https://docs.starrocks.io/docs/loading/Kafka-connector-starrocks/)

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

リリース日: 2023年12月19日

**機能**

オープンソースソフトウェアライセンスとしてApache Licenseを追加しました。 [#9](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/9)

#### 1.0.2

リリース日: 2023年12月14日

**機能**

ソースデータがDECIMAL型であることをサポートします。 [#7](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/7)

#### 1.0.1

リリース日: 2023年11月28日

**機能**

- Debeziumデータを主キーテーブルにロードすることをサポートします。 [#4](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/4)
- スキーマレジストリなしでJSONデータを解析することをサポートします。 [#6](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/6)

#### 1.0.0

リリース日: 2023年6月25日

**機能**

- CSV、JSON、Avro、Protobufデータのロードをサポートします。
- 自己管理のApache KafkaクラスターまたはConfluentクラウドからのデータロードをサポートします。