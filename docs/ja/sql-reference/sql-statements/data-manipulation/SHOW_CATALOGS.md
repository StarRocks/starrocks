---
displayed_sidebar: docs
---

# SHOW CATALOGS

## 説明

現在の StarRocks クラスター内のすべての catalog をクエリします。これには、内部 catalog と外部 catalog が含まれます。

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

次の表は、このステートメントによって返されるフィールドを説明しています。

| **Field** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Catalog       | catalog 名。                                            |
| Type          | catalog のタイプ。`default_catalog` の場合は `Internal` が返されます。外部 catalog の場合は、対応する catalog タイプが返されます。例えば `Hive`、`Hudi`、`Iceberg` などです。 |
| Comment       | catalog のコメント。StarRocks は外部 catalog にコメントを追加することをサポートしていません。そのため、外部 catalog の場合、値は `NULL` です。catalog が `default_catalog` の場合、コメントはデフォルトで `An internal catalog contains this cluster's self-managed tables.` です。`default_catalog` は StarRocks クラスター内で唯一の内部 catalog です。 |

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