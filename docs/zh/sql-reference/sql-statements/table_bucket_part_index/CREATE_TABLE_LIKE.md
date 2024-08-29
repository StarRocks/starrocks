---
displayed_sidebar: docs
---

# CREATE TABLE LIKE

## 功能

该语句用于创建一个表结构和另一张表完全相同的空表。

## 语法

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name
```

说明:

<<<<<<< HEAD:docs/zh/sql-reference/sql-statements/data-definition/CREATE_TABLE_LIKE.md
1. 复制的表结构包括 Column Defination、Partitions、Table Properties 等。
2. 用户需要对复制的原表有 `SELECT` 权限，权限控制请参考 [GRANT](../account-management/GRANT.md) 章节。
3. 支持复制 MySQL 等外表。
=======
```sql
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
[partition_desc]
[distribution_desc]
[PROPERTIES ("key" = "value",...)]
LIKE [database.]<source_table_name>
```

## 参数说明

- `TEMPORARY`：创建临时表。从 v3.3.1 版本开始，StarRocks 支持在 Default Catalog 中创建临时表。更多信息，请参见 [临时表](../../../table_design/StarRocks_table_design.md#临时表)。
- `database`：数据库。
- `table_name`：要创建的表的名称。有关表名的命令要求，参见[系统限制](../../System_limit.md)。
- `source_table_name`：要拷贝的表的名称。
- `partition_desc`：分区方式。更多信息，参见 [CREATE TABLE](CREATE_TABLE.md#partition_desc)。
- `distribution_desc`：分桶方式。更多信息，参见 [CREATE TABLE](CREATE_TABLE.md#distribution_desc)。
- `PROPERTIES`：表的属性。支持所有表属性。更多信息，参见 [ALTER TABLE](ALTER_TABLE.md#修改表的属性)。
>>>>>>> e06217c368 ([Doc] Ref docs (#50111)):docs/zh/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_LIKE.md

## 示例

1. 在 test1 库下创建一张表结构和 table1 相同的空表，表名为 table2。

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1;
    ```

2. 在 test2 库下创建一张表结构和 test1.table1 相同的空表，表名为 table2。

    ```sql
    CREATE TABLE test2.table2 LIKE test1.table1;
    ```

3. 在 test1 库下创建一张表结构和 MySQL 外表 table1 相同的空表，表名为 table2。

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1;
    ```
