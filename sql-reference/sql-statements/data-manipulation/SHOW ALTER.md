# SHOW ALTER

## 功能

查询当前正在进行的各类修改任务的执行情况。

## 语法

```sql
SHOW ALTER [TABLE [COLUMN | ROLLUP] [FROM db_name]];
```

## 参数说明

- `TABLE COLUMN`：可选。如果指定了COLUMN，该语句用于查询修改列的任务。如果需要嵌套WHERE子句，支持的语法为 `[WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]`。
- `TABLE ROLLUP`：可选。如果指定了ROLLUP，该语句用于查询创建或删除 ROLLUP index 的任务。
- `db_name`：可选。如果不指定 `db_name`，表示当前默认数据库。

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
