---
displayed_sidebar: docs
---

# Releases of StarRocks Connector for Kafka

## Notifications

**ユーザーガイド:** [Load data using Kafka connector](../loading/Kafka-connector-starrocks.md)

**ソースコード:** [starrocks-connector-for-kafka](https://github.com/StarRocks/starrocks-connector-for-kafka)

**圧縮ファイルの命名形式:** `starrocks-kafka-connector-${connector_version}.tar.gz`

**圧縮ファイルのダウンロードリンク:** [starrocks-kafka-connector](https://github.com/StarRocks/starrocks-connector-for-kafka/releases)

**バージョン要件:**

| Kafka Connector | StarRocks | Java |
| --------------- | --------- | ---- |
| 1.0.3           | 2.1 and later | 8    |
| 1.0.2           | 2.1 and later | 8    |
| 1.0.1           | 2.1 and later | 8    |
| 1.0.0           | 2.1 and later | 8    |

## リリースノート

### 1.0

#### 1.0.3

リリース日: 2023年12月19日

**機能**

Apache License をオープンソースソフトウェアライセンスとして追加しました。 [#9](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/9)

#### 1.0.2

リリース日: 2023年12月14日

**機能**

ソースデータが DECIMAL 型であることをサポートします。 [#7](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/7)

#### 1.0.1

リリース日: 2023年11月28日

**機能**

- Debezium データを主キーテーブルにロードすることをサポートします。 [#4](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/4)
- スキーマレジストリなしで JSON データを解析することをサポートします。 [#6](https://github.com/StarRocks/starrocks-connector-for-kafka/pull/6)

#### 1.0.0

リリース日: 2023年6月25日

**機能**

- CSV、JSON、Avro、Protobuf データのロードをサポートします。
- 自己管理の Apache Kafka クラスターまたは Confluent クラウドからのデータロードをサポートします。