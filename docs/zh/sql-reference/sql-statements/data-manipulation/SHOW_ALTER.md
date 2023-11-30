---
displayed_sidebar: "Chinese"
---

# SHOW ALTER TABLE

## 功能

查询正在进行的以下变更 (Alter) 任务的执行情况：

- 修改列
- 优化表结构（自 3.2）
- 创建和删除 Rollup 索引

## 语法

- 查询修改列或者优化表结构的任务执行情况

    ```sql
    SHOW ALTER TABLE { COLUMN | OPTIMIZE } [FROM db_name] [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
    ```

- 查询 Rollup 索引变更任务的执行情况。

    ```sql
    SHOW ALTER TABLE ROLLUP [FROM db_name]
    ```

## 参数说明

- `COLUMN ｜ OPTIMIZE | ROLLUP`：从 COLUMN、OPTIMIZE 和 ROLLUP 中必选其中一个。
  - 如果指定了 COLUMN 或者 OPTIMIZE，该语句用于查询修改列和优化表结构任务。
  - 如果指定了 ROLLUP，该语句用于查询创建或删除 ROLLUP 索引的任务。
- `db_name`：可选。如果不指定，则默认使用当前数据库。

## 示例

1. 查询当前数据库中所有修改列任务和优化表结构任务，以及创建或删除 ROLLUP 索引任务的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN;
    SHOW ALTER TABLE OPTIMIZE;
    SHOW ALTER TABLE ROLLUP;
    ```

2. 查询指定数据库中修改列任务和优化表结构任务，以及创建或删除 ROLLUP 索引任务的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN FROM example_db;
    SHOW ALTER TABLE OPTIMIZE FROM example_db;
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ````

3. 查询指定表中最近一次修改列任务或者优化任务的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
    SHOW ALTER TABLE OPTIMIZE WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1; 
    ```

## 相关参考

- [CREATE TABLE](../data-definition/CREATE_TABLE.md)
- [ALTER TABLE](../data-definition/ALTER_TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
