---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# SHOW ALTER TABLE

## 功能

查询正在进行的 Alter table 操作的执行情况，包括：

- 修改列。
- 优化表结构（自 3.2 版本起），包括修改分桶方式和分桶数量。
- 创建和删除 rollup index。

## 语法

- 查询修改列或者优化表结构的操作执行情况。

    ```sql
    SHOW ALTER TABLE { COLUMN | OPTIMIZE } [FROM <db_name>]
    [WHERE <where_condition> ] [ORDER BY <col_name> [ASC | DESC]] [LIMIT <num>]
    ```

- 查询创建和删除 rollup index 的操作执行情况。

    ```sql
    SHOW ALTER TABLE ROLLUP [FROM <db_name>]
    ```

## 参数说明

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`：从 `COLUMN`、`OPTIMIZE` 和 `ROLLUP` 中必选其中一个。
  - 如果指定了 `COLUMN`，该语句用于查询修改列的操作。
  - 如果指定了 `OPTIMIZE`，该语句用于查询优化表结构操作（修改分桶方式和分桶数量）。
  - 如果指定了 `ROLLUP`，该语句用于查询创建或删除 rollup index 的操作。
- 当指定了 `COLUMN` 或者 `OPTIMIZE` 查询修改列或者优化表结构操作时，支持使用如下子句：
  - `WHERE <where_condition>`：根据操作的 `TableName`、`CreateTime`、`FinishTime` 和 `State` 过滤出满足条件的操作。
  - `ORDER BY <col_name> [ASC | DESC]`：根据操作的 `TableName`、`CreateTime`、`FinishTime` 和 `State` 对返回结果中的操作进行排序。
  - `LIMIT <num>`：返回指定个数的操作。
- `db_name`：可选。如果不指定，则默认使用当前数据库。

## 示例

1. 查询当前数据库中所有修改列操作和优化表结构操作，以及创建或删除 rollup index 操作的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN;
    SHOW ALTER TABLE OPTIMIZE;
    SHOW ALTER TABLE ROLLUP;
    ```

2. 查询指定数据库中修改列操作和优化表结构操作，以及创建或删除 rollup index 操作的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN FROM example_db;
    SHOW ALTER TABLE OPTIMIZE FROM example_db;
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ````

3. 查询指定表中最近一次修改列操作或者优化表结构操作的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
    SHOW ALTER TABLE OPTIMIZE WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1; 
    ```

## 相关参考

- [CREATE TABLE](CREATE_TABLE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
