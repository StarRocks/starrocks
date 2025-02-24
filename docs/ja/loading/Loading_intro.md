---
displayed_sidebar: docs
toc_max_heading_level: 3
---

# ロードオプション

データロードは、ビジネス要件に基づいてさまざまなデータソースから生のデータをクレンジングおよび変換し、結果のデータを StarRocks にロードして分析を容易にするプロセスです。

StarRocks は、データロードのためにさまざまなオプションを提供しています:

- ロード方法: Insert、Stream Load、Broker Load、Pipe、Routine Load、Spark Load
- エコシステムツール: StarRocks Connector for Apache Kafka® (Kafka connector)、StarRocks Connector for Apache Spark™ (Spark connector)、StarRocks Connector for Apache Flink® (Flink connector)、および SMT、DataX、CloudCanal、Kettle Connector などの他のツール
- API: Stream Load トランザクションインターフェース

これらのオプションはそれぞれ独自の利点があり、独自のデータソースシステムをサポートしています。

このトピックでは、これらのオプションの概要を提供し、それらの比較を通じて、データソース、ビジネスシナリオ、データ量、データファイル形式、ロード頻度に基づいて選択するロードオプションを決定するのに役立てます。

## ロードオプションの紹介

このセクションでは、StarRocks で利用可能なロードオプションの特性とビジネスシナリオについて主に説明します。

![ロードオプションの概要](../_assets/loading_intro_overview.png)

:::note

以下のセクションでは、「バッチ」または「バッチロード」は、指定されたソースから一度に大量のデータを StarRocks にロードすることを指し、「ストリーム」または「ストリーミング」は、リアルタイムでデータを継続的にロードすることを指します。

:::

## ロード方法

### [Insert](InsertInto.md)

**ビジネスシナリオ:**

- INSERT INTO VALUES: 少量のデータを内部テーブルに追加します。
- INSERT INTO SELECT:
  - INSERT INTO SELECT FROM `<table_name>`: 内部または外部テーブルに対するクエリの結果をテーブルに追加します。
  - INSERT INTO SELECT FROM FILES(): リモートストレージ内のデータファイルに対するクエリの結果をテーブルに追加します。

    :::note

    AWS S3 では、この機能は v3.1 以降でサポートされています。HDFS、Microsoft Azure Storage、Google GCS、および S3 互換ストレージ (MinIO など) では、この機能は v3.2 以降でサポートされています。

    :::

**ファイル形式:**

- INSERT INTO VALUES: SQL
- INSERT INTO SELECT:
  - INSERT INTO SELECT FROM `<table_name>`: StarRocks テーブル
  - INSERT INTO SELECT FROM FILES(): Parquet および ORC

**データ量:** 固定されていません (データ量はメモリサイズに基づいて変動します。)

### [Stream Load](StreamLoad.md)

**ビジネスシナリオ:** ローカルファイルシステムからデータをバッチロードします。

**ファイル形式:** CSV および JSON

**データ量:** 10 GB 以下

### [Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)

**ビジネスシナリオ:**

- HDFS や AWS S3、Microsoft Azure Storage、Google GCS、S3 互換ストレージ (MinIO など) などのクラウドストレージからデータをバッチロードします。
- ローカルファイルシステムや NAS からデータをバッチロードします。

**ファイル形式:** CSV、Parquet、ORC、および JSON (v3.2.3 以降でサポート)

**データ量:** 数十 GB から数百 GB

### [Pipe](../sql-reference/sql-statements/loading_unloading/pipe/CREATE_PIPE.md)

**ビジネスシナリオ:** HDFS または AWS S3 からデータをバッチロードまたはストリームロードします。

:::note

このロード方法は v3.2 以降でサポートされています。

:::

**ファイル形式:** Parquet および ORC

**データ量:** 100 GB から 1 TB 以上

### [Routine Load](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md)

**ビジネスシナリオ:** Kafka からデータをストリームロードします。

**ファイル形式:** CSV、JSON、および Avro (v3.0.1 以降でサポート)

**データ量:** MB から GB のデータをミニバッチとして

### [Spark Load](../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md)

**ビジネスシナリオ:** Spark クラスターを使用して、HDFS に保存された Apache Hive™ テーブルのデータをバッチロードします。

**ファイル形式:** CSV、Parquet (v2.0 以降でサポート)、および ORC (v2.0 以降でサポート)

**データ量:** 数十 GB から TB

## エコシステムツール

### [Kafka connector](Kafka-connector-starrocks.md)

**ビジネスシナリオ:** Kafka からデータをストリームロードします。

### [Spark connector](Spark-connector-starrocks.md)

**ビジネスシナリオ:** Spark からデータをバッチロードします。

### [Flink connector](Flink-connector-starrocks.md)

**ビジネスシナリオ:** Flink からデータをストリームロードします。

### [SMT](../integrations/loading_tools/SMT.md)

**ビジネスシナリオ:** Flink を通じて MySQL、PostgreSQL、SQL Server、Oracle、Hive、ClickHouse、TiDB などのデータソースからデータをロードします。

### [DataX](../integrations/loading_tools/DataX-starrocks-writer.md)

**ビジネスシナリオ:** MySQL や Oracle などのリレーショナルデータベース、HDFS、Hive など、さまざまな異種データソース間でデータを同期します。

### [CloudCanal](../integrations/loading_tools/CloudCanal.md)

**ビジネスシナリオ:** MySQL、Oracle、PostgreSQL などのソースデータベースから StarRocks へのデータの移行または同期を行います。

### [Kettle Connector](https://github.com/StarRocks/starrocks-connector-for-kettle)

**ビジネスシナリオ:** Kettle と統合します。Kettle の強力なデータ処理および変換機能と StarRocks の高性能データストレージおよび分析能力を組み合わせることで、より柔軟で効率的なデータ処理ワークフローを実現できます。

## API

### [Stream Load トランザクションインターフェース](Stream_Load_transaction_interface.md)

**ビジネスシナリオ:** Flink や Kafka などの外部システムからデータをロードするトランザクションに対して、2 フェーズコミット (2PC) を実装し、高度に並行したストリームロードのパフォーマンスを向上させます。この機能は v2.4 以降でサポートされています。

**ファイル形式:** CSV および JSON

**データ量:** 10 GB 以下

## ロードオプションの選択

このセクションでは、一般的なデータソースに利用可能なロードオプションを一覧にし、あなたの状況に最適なオプションを選択するのに役立てます。

### オブジェクトストレージ

| **データソース**                       | **利用可能なロードオプション**                                |
| ------------------------------------- | ------------------------------------------------------------ |
| AWS S3                                | <ul><li>(バッチ) INSERT INTO SELECT FROM FILES() (v3.1 以降でサポート)</li><li>(バッチ) Broker Load</li><li>(バッチまたはストリーミング) Pipe (v3.2 以降でサポート)</li></ul>詳細は [Load data from AWS S3](s3.md) を参照してください。 |
| Microsoft Azure Storage               | <ul><li>(バッチ) INSERT INTO SELECT FROM FILES() (v3.2 以降でサポート)</li><li>(バッチ) Broker Load</li></ul>詳細は [Load data from Microsoft Azure Storage](azure.md) を参照してください。 |
| Google GCS                            | <ul><li>(バッチ) INSERT INTO SELECT FROM FILES() (v3.2 以降でサポート)</li><li>(バッチ) Broker Load</li></ul>詳細は [Load data from GCS](gcs.md) を参照してください。 |
| S3 互換ストレージ (MinIO など)         | <ul><li>(バッチ) INSERT INTO SELECT FROM FILES() (v3.2 以降でサポート)</li><li>(バッチ) Broker Load</li></ul>詳細は [Load data from MinIO](minio.md) を参照してください。 |

### ローカルファイルシステム (NAS を含む)

| **データソース**                   | **利用可能なロードオプション**                                |
| --------------------------------- | ------------------------------------------------------------ |
| ローカルファイルシステム (NAS を含む) | <ul><li>(バッチ) Stream Load</li><li>(バッチ) Broker Load</li></ul>詳細は [Load data from a local file system](StreamLoad.md) を参照してください。 |

### HDFS

| **データソース** | **利用可能なロードオプション**                                |
| --------------- | ------------------------------------------------------------ |
| HDFS            | <ul><li>(バッチ) INSERT INTO SELECT FROM FILES() (v3.2 以降でサポート)</li><li>(バッチ) Broker Load</li><li>(バッチまたはストリーミング) Pipe (v3.2 以降でサポート)</li></ul>詳細は [Load data from HDFS](hdfs_load.md) を参照してください。 |

### Flink、Kafka、および Spark

| **データソース** | **利用可能なロードオプション**                                |
| --------------- | ------------------------------------------------------------ |
| Apache Flink®   | <ul><li>[Flink connector](Flink-connector-starrocks.md)</li><li>[Stream Load トランザクションインターフェース](Stream_Load_transaction_interface.md)</li></ul> |
| Apache Kafka®   | <ul><li>(ストリーミング) [Kafka connector](Kafka-connector-starrocks.md)</li><li>(ストリーミング) [Routine Load](RoutineLoad.md)</li><li>[Stream Load トランザクションインターフェース](Stream_Load_transaction_interface.md)</li></ul> **NOTE**<br/>ソースデータがマルチテーブルジョインや ETL 操作を必要とする場合、Flink を使用してデータを読み取り、事前処理を行い、その後 [Flink connector](Flink-connector-starrocks.md) を使用してデータを StarRocks にロードできます。 |
| Apache Spark™   | <ul><li>[Spark connector](Spark-connector-starrocks.md)</li><li>[Spark Load](SparkLoad.md)</li></ul> |

### データレイク

| **データソース** | **利用可能なロードオプション**                                |
| --------------- | ------------------------------------------------------------ |
| Apache Hive™    | <ul><li>(バッチ) [Hive catalog](../data_source/catalog/hive_catalog.md) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。</li><li>(バッチ) [Spark Load](https://docs.starrocks.io/docs/loading/SparkLoad/).</li></ul> |
| Apache Iceberg  | (バッチ) [Iceberg catalog](../data_source/catalog/iceberg/iceberg_catalog.md) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。 |
| Apache Hudi     | (バッチ) [Hudi catalog](../data_source/catalog/hudi_catalog.md) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。 |
| Delta Lake      | (バッチ) [Delta Lake catalog](../data_source/catalog/deltalake_catalog.md) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。 |
| Elasticsearch   | (バッチ) [Elasticsearch catalog](../data_source/catalog/elasticsearch_catalog.md) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。 |
| Apache Paimon   | (バッチ) [Paimon catalog](../data_source/catalog/paimon_catalog.md) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。 |

StarRocks は、v3.2 以降で [unified catalogs](https://docs.starrocks.io/docs/data_source/catalog/unified_catalog/) を提供しており、Hive、Iceberg、Hudi、Delta Lake のデータソースからのテーブルを統合データソースとして取り扱うことができ、インジェストを必要としません。

### 内部および外部データベース

| **データソース**                                              | **利用可能なロードオプション**                                |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| StarRocks                                                    | (バッチ) [StarRocks external table](../data_source/External_table.md#starrocks-external-table) を作成し、その後 [INSERT INTO VALUES](InsertInto.md#insert-data-via-insert-into-values) を使用していくつかのデータレコードを挿入するか、[INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用してテーブルのデータを挿入します。<br/>**NOTE**<br/>StarRocks external tables はデータの書き込みのみをサポートしており、データの読み取りはサポートしていません。 |
| MySQL                                                        | <ul><li>(バッチ) [JDBC catalog](../data_source/catalog/jdbc_catalog.md) (推奨) または [MySQL external table](../data_source/External_table.md#deprecated-mysql-external-table) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。</li><li>(ストリーミング) [SMT、Flink CDC connector、Flink、および Flink connector](Flink_cdc_load.md) を使用します。</li></ul> |
| その他のデータベース (Oracle、PostgreSQL、SQL Server、ClickHouse、TiDB など) | <ul><li>(バッチ) [JDBC catalog](../data_source/catalog/jdbc_catalog.md) (推奨) または [JDBC external table](../data_source/External_table.md#external-table-for-a-jdbc-compatible-database) を作成し、その後 [INSERT INTO SELECT FROM `<table_name>`](InsertInto.md#insert-data-from-an-internal-or-external-table-into-an-internal-table) を使用します。</li><li>(ストリーミング) [SMT、Flink CDC connector、Flink、および Flink connector](loading_tools.md) を使用します。</li></ul> |