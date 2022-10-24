# SHOW ALTER TABLE

## 功能

该语句用于展示当前正在进行的各类修改任务的执行情况。

## 语法

注：方括号 [] 中内容可省略不写。

```sql
SHOW ALTER [TABLE [COLUMN | ROLLUP] [FROM db_name]];
```

说明：

```plain text

TABLE COLUMN：展示修改列的 ALTER 任务
支持语法[WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
TABLE ROLLUP：展示创建或删除 ROLLUP index 的任务
如果不指定 db_name，使用当前默认 db

```

## 示例

1. 展示默认 db 的所有修改列的任务执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN;
    ```

2. 展示某个表最近一次修改列的任务执行情况。

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 
    ```

3. 展示指定 db 的创建或删除 ROLLUP index 的任务执行情况。

    ```sql
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ````

## keyword

SHOW, ALTER
