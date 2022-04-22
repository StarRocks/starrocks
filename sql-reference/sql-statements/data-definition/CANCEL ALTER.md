# CANCEL ALTER

## 功能

该语句用于撤销一个 ALTER 操作。

## 语法

### 撤销 `ALTER TABLE COLUMN` 操作

```sql
CANCEL ALTER TABLE COLUMN
FROM db_name.table_name;
```

### 撤销 `ALTER TABLE ROLLUP` 操作

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name;
```

### 根据 job id 批量撤销 rollup 操作

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name (jobid,...);
```

注意：
该命令为 **异步** 操作，具体是否执行成功需要使用 `show alter table rollup` 查看任务状态确认。

### 撤销 ALTER CLUSTER 操作

```sql
（待实现...）
```

## 示例

1. 撤销针对 my_table 表的 `ALTER COLUMN` 操作。

    ```sql
    CANCEL ALTER TABLE COLUMN
    FROM example_db.my_table;
    ```

    [CANCEL ALTER TABLE ROLLUP]

2. 撤销 my_table 下的 `ADD ROLLUP` 操作。

    ```sql
    CANCEL ALTER TABLE ROLLUP
    FROM example_db.my_table;
    ```

    [CANCEL ALTER TABLE ROLLUP]

3. 根据 job id 撤销 my_table 下的 `ADD ROLLUP` 操作。

    ```sql
    CANCEL ALTER TABLE ROLLUP
    FROM example_db.my_table (12801,12802);
    ```

## keyword

CANCEL，ALTER，TABLE，COLUMN，ROLLUP
