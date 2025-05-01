---
displayed_sidebar: docs
---

# CREATE EXTERNAL CATALOG

## 説明

外部 catalog を作成します。外部 catalog を使用すると、データを StarRocks にロードしたり、外部テーブルを作成したりすることなく、外部データソースのデータをクエリできます。現在、次のタイプの外部 catalog を作成できます。

- [Hive catalog](../../../data_source/catalog/hive_catalog.md): Apache Hive™ からデータをクエリするために使用します。
- [Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md): Apache Iceberg からデータをクエリするために使用します。
- [Hudi catalog](../../../data_source/catalog/hudi_catalog.md): Apache Hudi からデータをクエリするために使用します。
- [Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md): Delta Lake からデータをクエリするために使用します。

外部 catalog を作成する前に、StarRocks クラスターを設定して、外部データソースのデータストレージシステム（Amazon S3 など）、メタデータサービス（Hive metastore など）、認証サービス（Kerberos など）の要件を満たすようにしてください。詳細については、各 [external catalog topic](../../../data_source/catalog/catalog_overview.md) の「始める前に」セクションを参照してください。

## 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
PROPERTIES ("key"="value", ...)
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | はい          | 外部 catalog の名前。命名規則は次のとおりです:<ul><li>名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。</li><li>名前は大文字と小文字を区別し、1023 文字を超えることはできません。</li></ul> |
| comment       | いいえ           | 外部 catalog の説明。 |
| PROPERTIES    | はい          | 外部 catalog のプロパティ。外部 catalog の種類に基づいてプロパティを設定します。詳細については、[Hive catalog](../../../data_source/catalog/hive_catalog.md)、[Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md)、[Hudi catalog](../../../data_source/catalog/hudi_catalog.md)、および [Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md) を参照してください。 |

## 例

例 1: `hive_metastore_catalog` という名前の Hive catalog を作成します。対応する Hive クラスターは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG hive_metastore_catalog
PROPERTIES(
   "type"="hive", 
   "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 2: `hive_glue_catalog` という名前の Hive catalog を作成します。対応する Hive クラスターは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG hive_glue_catalog
PROPERTIES(
    "type"="hive", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

例 3: `iceberg_metastore_catalog` という名前の Iceberg catalog を作成します。対応する Iceberg クラスターは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG iceberg_metastore_catalog
PROPERTIES(
    "type"="iceberg",
    "iceberg.catalog.type"="hive",
    "iceberg.catalog.hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 4: `iceberg_glue_catalog` という名前の Iceberg catalog を作成します。対応する Iceberg クラスターは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG iceberg_glue_catalog
PROPERTIES(
    "type"="iceberg", 
    "iceberg.catalog.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

例 5: `hudi_metastore_catalog` という名前の Hudi catalog を作成します。対応する Hudi クラスターは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG hudi_metastore_catalog
PROPERTIES(
    "type"="hudi",
    "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 6: `hudi_glue_catalog` という名前の Hudi catalog を作成します。対応する Hudi クラスターは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG hudi_glue_catalog
PROPERTIES(
    "type"="hudi", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

例 7: `delta_metastore_catalog` という名前の Delta Lake catalog を作成します。対応する Delta Lake サービスは、メタデータサービスとして Hive metastore を使用します。

```SQL
CREATE EXTERNAL CATALOG delta_metastore_catalog
PROPERTIES(
    "type"="deltalake",
    "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

例 8: `delta_glue_catalog` という名前の Delta Lake catalog を作成します。対応する Delta Lake サービスは、メタデータサービスとして AWS Glue を使用します。

```SQL
CREATE EXTERNAL CATALOG delta_glue_catalog
PROPERTIES(
    "type"="deltalake", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

## 参考文献

- StarRocks クラスター内のすべての catalog を表示するには、[SHOW CATALOGS](../data-manipulation/SHOW_CATALOGS.md) を参照してください。
- StarRocks クラスターから外部 catalog を削除するには、[DROP CATALOG](../data-definition/DROP_CATALOG.md) を参照してください。