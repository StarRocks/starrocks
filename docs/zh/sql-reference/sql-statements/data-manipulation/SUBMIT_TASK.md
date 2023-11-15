# SUBMIT TASK

## 功能

为 ETL 语句创建异步任务。此功能自 StarRocks 2.5 起支持。

StarRocks v3.0 支持为 [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) 和 [INSERT](../data-manipulation/insert.md) 创建异步任务。

您可以使用 [DROP TASK](./DROP_TASK.md) 删除异步任务。

## 语法

```SQL
SUBMIT TASK [task_name] AS <etl_statement>
```

## 参数说明

| **参数**      | **说明**                                                     |
| ------------- | ------------------------------------------------------------ |
| task_name     | 任务名称。                                                   |
| etl_statement | 需要创建异步任务的 ETL 语句。StarRocks 当前支持为 [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) 和 [INSERT](../data-manipulation/insert.md) 创建异步任务。 |

## 使用说明

该语句会创建一个 Task，表示一个 ETL 语句执行任务的存储模板。您可以通过查询 Information Schema 中的元数据表 `tasks` 来查看 Task 信息：

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

执行 Task 后会对应生成一个 TaskRun，表示一个 ETL 语句执行任务。TaskRun 有以下状态：

- `PENDING`：任务等待执行。
- `RUNNING`：任务正在执行。
- `FAILED`：任务执行失败。
- `SUCCESS`：任务执行成功。

您可以通过查询 Information Schema 中的元数据表 `task_runs` 来查看 TaskRun 状态：

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## 相关 FE 参数

您可以通过调整如下 FE 参数配置异步 ETL 任务：

| **参数**                   | **默认值** | **说明**                                                     |
| -------------------------- | ---------- | ------------------------------------------------------------ |
| task_ttl_second            | 259200     | Task 的有效期，单位秒。超过有效期的 Task 会被自动删除。        |
| task_check_interval_second | 14400      | 删除过期 Task 的间隔时间，单位秒。                           |
| task_runs_ttl_second       | 259200     | TaskRun 的有效期，单位秒。超过有效期的 TaskRun 会被自动删除。此外，成功和失败状态的 TaskRun 也会被自动删除。 |
| task_runs_concurrency      | 20         | 最多可同时运行的 TaskRun 的数量。                            |
| task_runs_queue_length     | 500        | 最多可同时等待运行的 TaskRun 的数量。如同时等待运行的 TaskRun 的数量超过该参数的默认值，您将无法继续执行 Task。 |

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
