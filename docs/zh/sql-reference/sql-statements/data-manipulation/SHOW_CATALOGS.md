---
displayed_sidebar: "Chinese"
---

# SHOW CATALOGS

## 功能

查看当前集群中的所有 catalog，包括 internal catalog 和 external catalog。

## 语法

```SQL
SHOW CATALOGS
```

## 返回结果说明

```SQL
+----------+--------+----------+
| Catalog  | Type   | Comment  |
+----------+--------+----------+
```

返回结果中的字段说明如下：

| **字段** | **说明**                                                     |
| -------- | ------------------------------------------------------------ |
| Catalog  | Catalog 名称。                                               |
| Type     | Catalog 类型。如果是 `default_catalog`，则返回 `Internal`。如果是 external catalog，则返回 external catalog 的类型，例如 `Hive`, `Hudi`, `Iceberg`。          |
| Comment  | Catalog 的备注。<ul><li>在创建 external catalog 时不支持为 external catalog 添加备注，所以如果是 external catalog，则返回的 `Comment` 为 `NULL`。</li><li>如果是 `default_catalog`，则默认返回的 `Comment` 为 `An internal catalog contains this cluster's self-managed tables.`。`default_catalog` 是 StarRocks 集群中唯一的 internal catalog，不允许删除。</li></ul> |

## 示例

查看当前集群中的所有 catalog。

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
