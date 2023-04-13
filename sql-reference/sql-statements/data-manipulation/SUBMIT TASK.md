# SUBMIT TASK

## 功能

为 ETL 语句创建异步任务。此功能自 StarRocks 3.0 起支持。

## 语法

```SQL
SUBMIT TASK [task_name] AS <etl_statement>
```

## 参数说明

| **参数**      | **说明**                                                     |
| ------------- | ------------------------------------------------------------ |
| task_name     | 任务名称。                                                   |
| etl_statement | 需要创建异步任务的 ETL 语句。StarRocks 当前支持为 [CREATE TABLE AS SELECT](../data-definition/CREATE%20TABLE%20AS%20SELECT.md) 和 [INSERT](../data-manipulation/insert.md) 创建异步任务。 |

## 注意事项

您可以通过查询 Information Schema 中的元数据表 `task_runs` 来查看异步任务的状态。

```SQL
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## 示例

示例一：为 `CREATE TABLE tbl1 AS SELECT * FROM src_tbl` 创建异步任务，并命名为 `etl0`：

```SQL
SUBMIT TASK etl0 AS CREATE TABLE tbl1 AS SELECT * FROM src_tbl;
```

示例二：为 `INSERT INTO tbl2 SELECT * FROM src_tbl` 创建异步任务，并命名为 `etl1`：

```SQL
SUBMIT TASK etl1 AS INSERT INTO tbl2 SELECT * FROM src_tbl;
```

示例三：为 `INSERT OVERWRITE tbl3 SELECT * FROM src_tbl` 创建异步任务：

```SQL
SUBMIT TASK AS INSERT OVERWRITE tbl3 SELECT * FROM src_tbl;
```

示例四：为 `INSERT OVERWRITE insert_wiki_edit SELECT * FROM source_wiki_edit` 创建异步任务，并通过 Hint 将 Query Timeout 设置为 `100000` 秒：

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```
