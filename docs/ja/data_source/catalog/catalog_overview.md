---
displayed_sidebar: docs
---

# 概要

このトピックでは、 catalog とは何か、および catalog を使用して内部データと外部データを管理およびクエリする方法について説明します。

StarRocks は v2.3 以降、 catalog 機能をサポートしています。 catalog を使用すると、1 つのシステムで内部データと外部データを管理でき、さまざまな外部システムに保存されているデータを簡単にクエリおよび分析できる柔軟な方法が提供されます。

## 基本概念

- **内部データ**: StarRocks に保存されているデータを指します。
- **外部データ**: Apache Hive™、Apache Iceberg、Apache Hudi、Delta Lake、JDBC などの外部データソースに保存されているデータを指します。

## Catalog

現在、StarRocks は、内部 catalog と外部 catalog の 2 種類の catalog を提供しています。

![figure1](../../_assets/3.8.1.png)

- **内部 catalog** は、StarRocks の内部データを管理します。たとえば、CREATE DATABASE または CREATE TABLE ステートメントを実行してデータベースまたはテーブルを作成すると、データベースまたはテーブルは内部 catalog に保存されます。各 StarRocks クラスタには、[default_catalog](../catalog/default_catalog.md) という名前の内部 catalog が 1 つだけあります。

- **外部 catalog** は、外部で管理されているメタストアへのリンクとして機能し、StarRocks に外部データソースへの直接アクセスを許可します。データのロードや移行を行わずに、外部データを直接クエリできます。現在、StarRocks は次のタイプの外部 catalog をサポートしています。
  - [Hive catalog](../catalog/hive_catalog.md): Hive からデータをクエリするために使用されます。
  - [Iceberg catalog](./iceberg/iceberg_catalog.md): Iceberg からデータをクエリするために使用されます。
  - [Hudi catalog](../catalog/hudi_catalog.md): Hudi からデータをクエリするために使用されます。
  - [Delta Lake catalog](../catalog/deltalake_catalog.md): Delta Lake からデータをクエリするために使用されます。
  - [JDBC catalog](../catalog/jdbc_catalog.md): JDBC 互換のデータソースからデータをクエリするために使用されます。
  - [Benchmark catalog](../catalog/benchmark_catalog.md): TPC-H、TPC-DS、および SSB スキーマ用に生成されたインフライトデータセットをクエリするために使用されます。
  - [Elasticsearch catalog](../catalog/elasticsearch_catalog.md): Elasticsearch からデータをクエリするために使用されます。 Elasticsearch catalog は v3.1 以降でサポートされています。
  - [Paimon catalog](../catalog/paimon_catalog.md): Paimon からデータをクエリするために使用されます。 Paimon catalog は v3.1 以降でサポートされています。
  - [Unified catalog](../catalog/unified_catalog.md): Hive、Iceberg、Hudi、および Delta Lake のデータソースから、統合されたデータソースとしてデータをクエリするために使用されます。 Unified catalog は v3.2 以降でサポートされています。

  StarRocks は、外部データをクエリするときに、外部データソースの次の 2 つのコンポーネントとやり取りします。

  - **メタストアサービス**: FE が外部データソースのメタデータにアクセスするために使用されます。 FE は、メタデータに基づいてクエリ実行プランを生成します。
  - **データストレージシステム**: 外部データを保存するために使用されます。分散ファイルシステムとオブジェクトストレージシステムの両方を、さまざまな形式のデータファイルを保存するためのデータストレージシステムとして使用できます。 FE がクエリ実行プランをすべての BE または CN に配布した後、すべての BE または CN はターゲットの外部データを並行してスキャンし、計算を実行して、クエリ結果を返します。

## Catalog へのアクセス

[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) ステートメントを使用して、現在のセッションで指定された catalog に切り替えることができます。次に、その catalog を使用してデータをクエリできます。

## データのクエリ

### 内部データのクエリ

StarRocks でデータをクエリするには、[Default catalog](../catalog/default_catalog.md) を参照してください。

### 外部データのクエリ

外部データソースからデータをクエリするには、[Query external data](../catalog/query_external_data.md) を参照してください。

### クロス catalog クエリ

現在の catalog からクロス catalog のフェデレーションクエリを実行するには、クエリするデータを `catalog_name.database_name` または `catalog_name.database_name.table_name` 形式で指定します。

- 現在のセッションが `default_catalog.olap_db` の場合、`hive_db` 内の `hive_table` をクエリします。

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table;
    ```

- 現在のセッションが `hive_catalog.hive_db` の場合、`default_catalog` 内の `olap_table` をクエリします。

   ```SQL
    SELECT * FROM default_catalog.olap_db.olap_table;
    ```

- 現在のセッションが `hive_catalog.hive_db` の場合、`hive_catalog` 内の `hive_table` と `default_catalog` 内の `olap_table` に対して JOIN クエリを実行します。

    ```SQL
    SELECT * FROM hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```

- 現在のセッションが別の catalog の場合、JOIN 句を使用して、`hive_catalog` 内の `hive_table` と `default_catalog` 内の `olap_table` に対して JOIN クエリを実行します。

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```