# SHOW ALTER TABLE

## 功能

查询当前正在进行的各类修改任务的执行情况。

## 语法

```sql
SHOW ALTER TABLE {COLUMN | ROLLUP} [FROM db_name]
```

## 参数说明

- `COLUMN ｜ ROLLUP`：表示从 COLUMN 和 ROLLUP 中必选其中一个。
  - 如果指定了 COLUMN，该语句用于查询修改列的任务。如果需要嵌套WHERE子句，支持的语法为 `[WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]`。
  - 如果指定了 ROLLUP，该语句用于查询创建或删除 ROLLUP index 的任务。
- `db_name`：可选。如果不指定，则默认使用当前数据库。

## 示例

1. 查询默认数据库的所有修改列任务的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN;
    ```

2. 查询某个表最近一次修改列任务的执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
    ```

3. 查询指定数据库的创建或删除 ROLLUP index 的任务执行情况。

    ```sql
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ````

## 相关参考

- [CREATE TABLE](../data-definition/CREATE%20TABLE.md)
- [ALTER TABLE](../data-definition/ALTER%20TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW%20TABLES.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW%20CREATE%20TABLE.md)
