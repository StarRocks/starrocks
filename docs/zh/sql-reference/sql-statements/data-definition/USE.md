---
displayed_sidebar: "Chinese"
---

# USE

## 功能

指定会话使用的数据库。指定数据库后，即可以进行后续的建表或者查询等操作。

## 语法

```SQL
USE [<catalog_name>.]<db_name>
```

## 参数说明

| **参数**     | **必选** | **说明**                                                     |
| ------------ | -------- | ------------------------------------------------------------ |
| catalog_name | 否       | Catalog 名称。<ul><li>如不指定该参数，则默认使用 `default_catalog` 下的数据库。</li><li>如要使用 external catalog 下的数据库，则必须指定该参数，具体见「示例二」。</li><li>如从一个 catalog 下的数据库切换到另一个 catalog 下的数据库，则必须指定该参数，具体见「示例三」。</li></ul>更多有关 catalog 的信息，请参见[概述](../../../data_source/catalog/catalog_overview.md)。 |
| db_name      | 是       | 数据库名称。该数据库必须存在。                               |

## 示例

示例一：使用 `default_catalog` 下的 `example_db` 作为会话的数据库。

```SQL
USE default_catalog.example_db;
```

或

```SQL
USE example_db;
```

示例二：使用 `hive_catalog` 下的 `example_db` 作为会话的数据库。

```SQL
USE hive_catalog.example_db;
```

示例三：将会话使用的数据库 `hive_catalog.example_table1` 切换到  `iceberg_catalog.example_table2`。

```SQL
USE iceberg_catalog.example_table2;
```
