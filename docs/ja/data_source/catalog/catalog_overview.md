---
displayed_sidebar: docs
---

# Overview

このトピックでは、catalog とは何か、そして catalog を使用して内部データと外部データを管理およびクエリする方法について説明します。

StarRocks は v2.3 以降で catalog 機能をサポートしています。catalog により、内部データと外部データを 1 つのシステムで管理でき、さまざまな外部システムに保存されているデータを簡単にクエリおよび分析する柔軟な方法を提供します。

## Basic concepts

- **Internal data**: StarRocks に保存されているデータを指します。
- **External data**: Apache Hive™、Apache Iceberg、Apache Hudi、Delta Lake、JDBC などの外部データソースに保存されているデータを指します。

## Catalog

現在、StarRocks は internal catalog と external catalog の 2 種類の catalog を提供しています。

![figure1](../../_assets/3.8.1.png)

- **Internal catalog** は StarRocks の内部データを管理します。たとえば、CREATE DATABASE または CREATE TABLE ステートメントを実行してデータベースやテーブルを作成すると、そのデータベースやテーブルは internal catalog に保存されます。各 StarRocks クラスターには [default_catalog](../catalog/default_catalog.md) という名前の internal catalog が 1 つだけあります。

- **External catalog** は外部で管理されているメタストアへのリンクのように機能し、StarRocks に外部データソースへの直接アクセスを提供します。データロードや移行なしで外部データを直接クエリできます。現在、StarRocks は以下の種類の external catalog をサポートしています:
  - [Hive catalog](../catalog/hive_catalog.md): Hive からデータをクエリするために使用されます。
  - [Iceberg catalog](../catalog/iceberg_catalog.md): Iceberg からデータをクエリするために使用されます。
  - [Hudi catalog](../catalog/hudi_catalog.md): Hudi からデータをクエリするために使用されます。
  - [Delta Lake catalog](../catalog/deltalake_catalog.md): Delta Lake からデータをクエリするために使用されます。
  - [JDBC catalog](../catalog/jdbc_catalog.md): JDBC 互換のデータソースからデータをクエリするために使用されます。
  - [Elasticsearch catalog](../catalog/elasticsearch_catalog.md): Elasticsearch からデータをクエリするために使用されます。Elasticsearch catalog は v3.1 以降でサポートされています。
  - [Paimon catalog](../catalog/paimon_catalog.md): Paimon からデータをクエリするために使用されます。Paimon catalog は v3.1 以降でサポートされています。
  - [Unified catalog](../catalog/unified_catalog.md): Hive、Iceberg、Hudi、Delta Lake データソースから統合データソースとしてデータをクエリするために使用されます。Unified catalog は v3.2 以降でサポートされています。

  StarRocks は外部データソースをクエリする際に、以下の 2 つのコンポーネントとやり取りします:

  - **Metastore service**: 外部データソースのメタデータにアクセスするために FEs によって使用されます。FEs はメタデータに基づいてクエリ実行計画を生成します。
  - **Data storage system**: 外部データを保存するために使用されます。分散ファイルシステムとオブジェクトストレージシステムの両方がデータストレージシステムとして使用され、さまざまな形式のデータファイルを保存できます。FEs がクエリ実行計画をすべての BEs または CNs に配布した後、すべての BEs または CNs がターゲットの外部データを並行してスキャンし、計算を実行し、クエリ結果を返します。

## Access catalog

[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) ステートメントを使用して、現在のセッションで指定された catalog に切り替えることができます。その後、その catalog を使用してデータをクエリできます。

## Query data

### Query internal data

StarRocks 内のデータをクエリするには、[Default catalog](../catalog/default_catalog.md) を参照してください。

### Query external data

外部データソースからデータをクエリするには、[Query external data](../catalog/query_external_data.md) を参照してください。

### Cross-catalog query

現在の catalog からクロス catalog フェデレーテッドクエリを実行するには、`catalog_name.database_name` または `catalog_name.database_name.table_name` 形式でクエリしたいデータを指定します。

- 現在のセッションが `default_catalog.olap_db` のときに `hive_db` の `hive_table` をクエリします。

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table;
    ```

- 現在のセッションが `hive_catalog.hive_db` のときに `default_catalog` の `olap_table` をクエリします。

   ```SQL
    SELECT * FROM default_catalog.olap_db.olap_table;
    ```

- 現在のセッションが `hive_catalog.hive_db` のときに `hive_catalog` の `hive_table` と `default_catalog` の `olap_table` に対して JOIN クエリを実行します。

    ```SQL
    SELECT * FROM hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```

- 現在のセッションが別の catalog のときに `hive_catalog` の `hive_table` と `default_catalog` の `olap_table` に対して JOIN 句を使用して JOIN クエリを実行します。

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```