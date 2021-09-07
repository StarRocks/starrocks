# CANCEL ALTER

## description

该语句用于撤销一个 ALTER 操作。
1.撤销 ALTER TABLE COLUMN 操作

语法：

```sql
CANCEL ALTER TABLE COLUMN
FROM db_name.table_name
```

2.撤销 ALTER TABLE ROLLUP 操作

语法：

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name
```

3.根据job id批量撤销rollup操作

语法：

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name (jobid,...)
```

注意：
该命令为异步操作，具体是否执行成功需要使用`show alter table rollup`查看任务状态确认

4.撤销 ALTER CLUSTER 操作

语法：

```sql
（待实现...）
```

## example

[CANCEL ALTER TABLE COLUMN]

1. 撤销针对 my_table 的 ALTER COLUMN 操作。

    ```sql
    CANCEL ALTER TABLE COLUMN
    FROM example_db.my_table;
    ```

    [CANCEL ALTER TABLE ROLLUP]

2. 撤销 my_table 下的 ADD ROLLUP 操作。

    ```sql
    CANCEL ALTER TABLE ROLLUP
    FROM example_db.my_table;
    ```

    [CANCEL ALTER TABLE ROLLUP]

3. 根据job id撤销 my_table 下的 ADD ROLLUP 操作。

    ```sql
    CANCEL ALTER TABLE ROLLUP
    FROM example_db.my_table (12801,12802);
    ```

## keyword

CANCEL,ALTER,TABLE,COLUMN,ROLLUP
