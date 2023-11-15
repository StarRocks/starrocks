# CREATE TABLE LIKE

## 功能

该语句用于创建一个表结构和另一张表完全相同的空表。

## 语法

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name
```

注：方括号 [] 中内容可省略不写。

说明:

1. 复制的表结构包括 Column Defination、Partitions、Table Properties 等。
2. 用户需要对复制的原表有 `SELECT` 权限，权限控制请参考 [GRANT](../account-management/GRANT.md) 章节。
3. 支持复制 MySQL 等外表。

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

## keyword

CREATE, TABLE, LIKE
