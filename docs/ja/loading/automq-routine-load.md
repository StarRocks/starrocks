---
description: クラウドベースの Kafka from AutoMQ
displayed_sidebar: docs
---

# AutoMQ Kafka

import Replicanum from '../_assets/commonMarkdown/replicanum.md'

[AutoMQ for Kafka](https://www.automq.com/docs) は、クラウド環境向けに再設計された Kafka のクラウドネイティブバージョンです。AutoMQ Kafka は [オープンソース](https://github.com/AutoMQ/automq-for-kafka) であり、Kafka プロトコルと完全に互換性があり、クラウドの利点を最大限に活用しています。自己管理型の Apache Kafka と比較して、AutoMQ Kafka はクラウドネイティブなアーキテクチャにより、容量の自動スケーリング、ネットワークトラフィックの自己バランス、パーティションの数秒での移動などの機能を提供します。これらの機能は、ユーザーにとっての総所有コスト (TCO) を大幅に削減します。

この記事では、StarRocks Routine Load を使用して AutoMQ Kafka にデータをインポートする方法を説明します。Routine Load の基本原則については、Routine Load Fundamentals のセクションを参照してください。

## 環境の準備

### StarRocks とテストデータの準備

StarRocks クラスターが稼働していることを確認してください。

テスト用にデータベースと主キーテーブルを作成します。

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

## AutoMQ Kafka とテストデータの準備

AutoMQ Kafka 環境とテストデータを準備するには、AutoMQ [Quick Start](https://www.automq.com/docs) ガイドに従って AutoMQ Kafka クラスターをデプロイします。StarRocks が AutoMQ Kafka サーバーに直接接続できることを確認してください。

AutoMQ Kafka で `example_topic` という名前のトピックをすばやく作成し、テスト JSON データを書き込むには、次の手順に従います。

### トピックの作成

Kafka のコマンドラインツールを使用してトピックを作成します。Kafka 環境にアクセスでき、Kafka サービスが稼働していることを確認してください。トピックを作成するためのコマンドは次のとおりです。

```shell
./kafka-topics.sh --create --topic example_topic --bootstrap-server 10.0.96.4:9092 --partitions 1 --replication-factor 1
```

> 注: `topic` と `bootstrap-server` を Kafka サーバーのアドレスに置き換えてください。

トピック作成の結果を確認するには、次のコマンドを使用します。

```shell
./kafka-topics.sh --describe example_topic --bootstrap-server 10.0.96.4:9092
```

### テストデータの生成

シンプルな JSON 形式のテストデータを生成します。

```json
{
  "id": 1,
  "name": "testuser",
  "timestamp": "2023-11-10T12:00:00",
  "status": "active"
}
```

### テストデータの書き込み

Kafka のコマンドラインツールまたはプログラミング手法を使用して、example_topic にテストデータを書き込みます。コマンドラインツールを使用した例は次のとおりです。

```shell
echo '{"id": 1, "name": "testuser", "timestamp": "2023-11-10T12:00:00", "status": "active"}' | sh kafka-console-producer.sh --broker-list 10.0.96.4:9092 --topic example_topic
```

> 注: `topic` と `bootstrap-server` を Kafka サーバーのアドレスに置き換えてください。

最近書き込まれたトピックデータを表示するには、次のコマンドを使用します。

```shell
sh kafka-console-consumer.sh --bootstrap-server 10.0.96.4:9092 --topic example_topic --from-beginning
```

## Routine Load タスクの作成

StarRocks コマンドラインで、AutoMQ Kafka トピックからデータを継続的にインポートする Routine Load タスクを作成します。

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

> 注: `kafka_broker_list` を Kafka サーバーのアドレスに置き換えてください。

### パラメータの説明

#### データ形式

PROPERTIES 句の "format" = "json" でデータ形式を JSON と指定します。

#### データの抽出と変換

ソースデータとターゲットテーブル間のマッピングと変換の関係を指定するには、COLUMNS および jsonpaths パラメータを設定します。COLUMNS の列名はターゲットテーブルの列名に対応し、その順序はソースデータの列順に対応します。jsonpaths パラメータは、JSON データから必要なフィールドデータを抽出するために使用され、新しく生成された CSV データに似ています。その後、COLUMNS パラメータは一時的に jsonpaths のフィールドに順番に名前を付けます。データ変換の詳細については、[Data Transformation during Import](./Etl_in_loading.md) を参照してください。
> 注: 各 JSON オブジェクトの各行が、ターゲットテーブルの列に対応するキー名と数量（順序は不要）を持っている場合、COLUMNS を設定する必要はありません。

## データインポートの確認

まず、Routine Load インポートジョブを確認し、Routine Load インポートタスクのステータスが RUNNING ステータスであることを確認します。

```sql
show routine load\G
```

次に、StarRocks データベース内の対応するテーブルをクエリすることで、データが正常にインポートされたことを確認できます。

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