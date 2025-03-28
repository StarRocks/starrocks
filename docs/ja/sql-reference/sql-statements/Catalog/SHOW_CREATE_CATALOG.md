---
displayed_sidebar: docs
---

# SHOW CREATE CATALOG

## 説明

[external catalog](../../../data_source/catalog/catalog_overview.md) の作成ステートメントを照会します。

返される結果の認証関連情報は匿名化されます。

このコマンドは v3.0 以降でサポートされています。

## 構文

```SQL
SHOW CREATE CATALOG <catalog_name>;
```

## パラメーター

| **パラメーター** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | はい          | 作成ステートメントを表示したい catalog の名前。 |

## 返される結果

```Plain
+------------+-----------------+
| Catalog    | Create Catalog  |
+------------+-----------------+
```

| **フィールド**  | **説明**                                        |
| -------------- | ------------------------------------------------------ |
| Catalog        | catalog の名前。                               |
| Create Catalog | catalog を作成するために実行されたステートメント。 |

## 例

次の例は、`hive_catalog_hms` という名前の Hive catalog の作成ステートメントを照会します。

```SQL
SHOW CREATE CATALOG hive_catalog_hms;
```

返される結果は次のとおりです。

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