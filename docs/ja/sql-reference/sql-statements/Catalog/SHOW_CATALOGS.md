---
displayed_sidebar: docs
---

# SHOW CATALOGS

## 説明

現在の StarRocks クラスター内のすべての catalog をクエリします。内部 catalog と external catalog を含みます。

> **注意**
>
> SHOW CATALOGS は、特定の external catalog に対して USAGE 権限を持つユーザーに対して external catalog を返します。ユーザーまたはロールがどの external catalog にもこの権限を持たない場合、このコマンドは default_catalog のみを返します。

## 構文

```SQL
SHOW CATALOGS
```

## 出力

```SQL
+----------+--------+----------+
| Catalog  | Type   | Comment  |
+----------+--------+----------+
```

このステートメントによって返されるフィールドを以下の表で説明します。

| **フィールド** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| Catalog       | catalog 名。                                            |
| Type          | catalog のタイプ。catalog が `default_catalog` の場合は `Internal` が返されます。catalog が external catalog の場合、例えば `Hive`、`Hudi`、`Iceberg` などの対応する catalog タイプが返されます。 |
| Comment       | catalog のコメント。StarRocks は external catalog にコメントを追加することをサポートしていません。そのため、external catalog の場合、値は `NULL` です。catalog が `default_catalog` の場合、コメントはデフォルトで `An internal catalog contains this cluster's self-managed tables.` です。`default_catalog` は StarRocks クラスター内で唯一の内部 catalog です。 |

## 例

現在のクラスター内のすべての catalog をクエリします。

```SQL
SHOW CATALOGS\G
*************************** 1. row ***************************
Catalog: default_catalog
   Type: Internal
Comment: An internal catalog contains this cluster's self-managed tables.
*************************** 2. row ***************************
Catalog: hudi_catalog
   Type: Hudi
Comment: NULL
*************************** 3. row ***************************
Catalog: iceberg_catalog
   Type: Iceberg
Comment: NULL
```