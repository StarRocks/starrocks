---
displayed_sidebar: docs
---

# SHOW CREATE CATALOG

## 説明

Hive、Iceberg、Hudi、Delta Lake カタログなどの external catalog の作成ステートメントを照会します。 [Hive Catalog](../../../data_source/catalog/hive_catalog.md)、 [Iceberg Catalog](../../../data_source/catalog/iceberg_catalog.md)、 [Hudi Catalog](../../../data_source/catalog/hudi_catalog.md)、 [Delta Lake Catalog](../../../data_source/catalog/deltalake_catalog.md) を参照してください。返される結果の認証関連情報は匿名化されます。

このコマンドは v2.5.4 以降でサポートされています。

## 構文

```SQL
SHOW CREATE CATALOG <catalog_name>;
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | はい          | 作成ステートメントを表示したいカタログの名前。 |

## 戻り結果

```Plain
+------------+-----------------+
| Catalog    | Create Catalog  |
+------------+-----------------+
```

| **フィールド**  | **説明**                                        |
| -------------- | ------------------------------------------------------ |
| Catalog        | カタログの名前。                               |
| Create Catalog | カタログを作成するために実行されたステートメント。 |

## 例

次の例は、`hive_catalog_hms` という名前の Hive カタログの作成ステートメントを照会します。

```SQL
SHOW CREATE CATALOG hive_catalog_hms;
```

戻り結果は次のとおりです。

```SQL
CREATE EXTERNAL CATALOG `hive_catalog_hms`
PROPERTIES ("aws.s3.access_key"  =  "AK******M4",
"hive.metastore.type"  =  "glue",
"aws.s3.secret_key"  =  "iV******iD",
"aws.glue.secret_key"  =  "iV******iD",
"aws.s3.use_instance_profile"  =  "false",
"aws.s3.region"  =  "us-west-1",
"aws.glue.region"  =  "us-west-1",
"type"  =  "hive",
"aws.glue.access_key"  =  "AK******M4"
)
```