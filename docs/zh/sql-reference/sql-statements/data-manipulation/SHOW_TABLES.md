---
displayed_sidebar: "Chinese"
---

# SHOW TABLES

## 功能

查看指定数据库下的所有表。可以是 StarRocks 内部表或外部数据源中的表。

> **注意**
>
> 要查看外部数据源中的表，必须有对应 External Catalog 的 USAGE 权限。

## 语法

```sql
SHOW TABLES [FROM <catalog_name>.<db_name>]
```

## 参数说明

| **参数**          | **必选** | **说明**                                                     |
| ----------------- | -------- | ------------------------------------------------------------ |
| catalog_name | 否       | Internal catalog 或 External catalog 的名称。<ul><li>如果不指定或指定为 internal catalog 名称（`default_catalog`），则查看当前 StarRocks 集群中的数据库。</li><li>如果指定 external catalog 名称，则查看外部数据源中的数据库。</li></ul> 您可以使用 [SHOW CATALOGS](SHOW_CATALOGS.md) 命令查看当前集群的内外部 catalog。|
| db_name | 否       | 数据库名称。如果不指定，默认为当前数据库。 |

## 示例

示例一：连接到 StarRocks 集群后，查看 Internal catalog 下数据库 `example_db` 中的表。下面两个语句功能对等。

```plain
show tables from example_db;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+

show tables from default_catalog.example_db;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+
```

示例二：使用 `USE <db_name>;` 连接到 `example_db` 数据库后，查看当前数据库下的表。

```plain
show tables;

+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+
```

示例三：查看 External catalog `hudi_catalog` 下指定数据库 `hudi_db` 中的表。

```plain
show tables from hudi_catalog.hudi_db;
+----------------------------+
| Tables_in_hudi_db          |
+----------------------------+
| hudi_sync_mor              |
| hudi_table1                |
+----------------------------+
```

您也可以通过 `SET CATALOG <catalog_name>;` 切换到 `hudi_catalog` 下，然后执行 `SHOW TABLES FROM hudi_db;` 来查看该数据库下的表。

## 相关文档

- [SHOW CATALOGS](SHOW_CATALOGS.md)：查看当前集群下所有的 Catalog。
- [SHOW DATABASES](SHOW_DATABASES.md)：查看 Internal Catalog 或 External Catalog 下的所有数据库。
- [SET CATALOG](../data-definition/SET_CATALOG.md)：切换 Catalog。
