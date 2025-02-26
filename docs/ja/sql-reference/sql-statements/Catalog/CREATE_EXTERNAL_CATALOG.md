---
displayed_sidebar: docs
---

# CREATE EXTERNAL CATALOG

## 説明

外部カタログを作成します。外部カタログを使用すると、データを StarRocks にロードしたり外部テーブルを作成したりすることなく、外部データソースのデータをクエリできます。現在、以下のタイプの外部カタログを作成できます。

- [Hive catalog](../../../data_source/catalog/hive_catalog.md): Apache Hive™ からデータをクエリするために使用されます。
- [Iceberg catalog](../../../data_source/catalog/iceberg/iceberg_catalog.md): Apache Iceberg からデータをクエリするために使用されます。
- [Hudi catalog](../../../data_source/catalog/hudi_catalog.md): Apache Hudi からデータをクエリするために使用されます。
- [Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md): Delta Lake からデータをクエリするために使用されます。
- [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md): JDBC 互換のデータソースからデータをクエリするために使用されます。
- [Elasticsearch catalog](../../../data_source/catalog/elasticsearch_catalog.md): Elasticsearch からデータをクエリするために使用されます。Elasticsearch カタログは v3.1 以降でサポートされています。
- [Paimon catalog](../../../data_source/catalog/paimon_catalog.md): Apache Paimon からデータをクエリするために使用されます。Paimon カタログは v3.1 以降でサポートされています。
- [Unified catalog](../../../data_source/catalog/unified_catalog.md): Hive、Iceberg、Hudi、および Delta Lake データソースを統合データソースとしてクエリするために使用されます。Unified カタログは v3.2 以降でサポートされています。

> **NOTE**
>
> - v3.0 以降、このステートメントには SYSTEM レベルの CREATE EXTERNAL CATALOG 権限が必要です。
> - 外部カタログを作成する前に、StarRocks クラスターを設定して、外部データソースのデータストレージシステム（Amazon S3 など）、メタデータサービス（Hive metastore など）、および認証サービス（Kerberos など）の要件を満たすようにしてください。詳細については、各 [external catalog topic](../../../data_source/catalog/catalog_overview.md) の「開始する前に」セクションを参照してください。

## 構文

```SQL
CREATE EXTERNAL CATALOG [IF NOT EXISTS] <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

## パラメータ

| **パラメータ** | **必須** | **説明** |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | はい          | 外部カタログの名前。命名規則については、[System limits](../../System_limit.md) を参照してください。 |
| comment       | いいえ           | 外部カタログの説明。 |
| PROPERTIES    | はい          | 外部カタログのプロパティ。外部カタログの種類に基づいてプロパティを設定します。詳細については、[Hive catalog](../../../data_source/catalog/hive_catalog.md)、[Iceberg catalog](../../../data_source/catalog/iceberg/iceberg_catalog.md)、[Hudi catalog](../../../data_source/catalog/hudi_catalog.md)、[Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md)、および [JDBC Catalog](../../../data_source/catalog/jdbc_catalog.md) を参照してください。 |

## 例

例 1: `hive_metastore_catalog` という名前の Hive カタログを作成します。対応する Hive クラスターは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG hive_metastore_catalog
COMMENT "External catalog to Hive"
PROPERTIES(
   "type"="hive", 
   "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 2: `hive_glue_catalog` という名前の Hive カタログを作成します。対応する Hive クラスターは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG hive_glue_catalog
COMMENT "External catalog to Hive"
PROPERTIES(
    "type"="hive", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

例 3: `iceberg_metastore_catalog` という名前の Iceberg カタログを作成します。対応する Iceberg クラスターは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG iceberg_metastore_catalog
COMMENT "External catalog to Iceberg"
PROPERTIES(
    "type"="iceberg",
    "iceberg.catalog.type"="hive",
    "iceberg.catalog.hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 4: `iceberg_glue_catalog` という名前の Iceberg カタログを作成します。対応する Iceberg クラスターは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG iceberg_glue_catalog
COMMENT "External catalog to Iceberg"
PROPERTIES(
    "type"="iceberg", 
    "iceberg.catalog.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

例 5: `hudi_metastore_catalog` という名前の Hudi カタログを作成します。対応する Hudi クラスターは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG hudi_metastore_catalog
COMMENT "External catalog to Hudi"
PROPERTIES(
    "type"="hudi",
    "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 6: `hudi_glue_catalog` という名前の Hudi カタログを作成します。対応する Hudi クラスターは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG hudi_glue_catalog
COMMENT "External catalog to Hudi"
PROPERTIES(
    "type"="hudi", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

例 7: `delta_metastore_catalog` という名前の Delta Lake カタログを作成します。対応する Delta Lake サービスは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG delta_metastore_catalog
COMMENT "External catalog to Delta"
PROPERTIES(
    "type"="deltalake",
    "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 8: `delta_glue_catalog` という名前の Delta Lake カタログを作成します。対応する Delta Lake サービスは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG delta_glue_catalog
COMMENT "External catalog to Delta"
PROPERTIES(
    "type"="deltalake", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

## 参考文献

- StarRocks クラスター内のすべてのカタログを表示するには、[SHOW CATALOGS](SHOW_CATALOGS.md) を参照してください。
- 外部カタログの作成ステートメントを表示するには、[SHOW CREATE CATALOG](SHOW_CREATE_CATALOG.md) を参照してください。
- StarRocks クラスターから外部カタログを削除するには、[DROP CATALOG](DROP_CATALOG.md) を参照してください。