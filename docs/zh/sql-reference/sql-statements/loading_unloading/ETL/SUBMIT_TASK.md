---
displayed_sidebar: docs
---

# SUBMIT TASK

## 功能

为 ETL 语句创建异步任务。

您可将该语句用于以下场景：

- 创建长时间运行的后台任务（从 v2.5 开始支持）
- 创建定期执行的任务（从 v3.3 开始支持）

支持的语句包括：

- [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md)（从 v3.0 开始支持）
- [INSERT](../INSERT.md)（从 v3.0 开始支持）
- [CACHE SELECT](../../../../data_source/data_cache_warmup.md)（从 v3.3 开始支持）

您可以通过查询 `INFORMATION_SCHEMA.tasks` 查看任务列表，或通过查询 `INFORMATION_SCHEMA.task_runs` 查看任务的执行历史。有关更多信息，请参阅[使用说明](#使用说明)。

您可以使用 [DROP TASK](DROP_TASK.md) 删除异步任务。

## 语法

```SQL
SUBMIT TASK <task_name> 
[SCHEDULE [START(<schedule_start>)] EVERY(INTERVAL <schedule_interval>) ]
[PROPERTIES(<"key" = "value"[, ...]>)]
AS <etl_statement>
```

## 参数说明

| **参数**      | **是否必须** | **描述**                                                                                     |
| ------------- | ------------ | ----------------------------------------------------------------------------------------- |
| task_name          | 是      | 任务名称。                                                                                   |
| schedule_start     | 否      | 定时任务的开始时间。                                                                           |
| schedule_interval  | 否      | 定时任务的执行间隔，最小间隔为 10 秒。                                                           |
| etl_statement      | 是      | 需要创建异步任务的 ETL 语句。StarRocks 当前支持为 [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) 和 [INSERT](../INSERT.md) |

## 使用说明

该语句会创建一个 Task，表示一个 ETL 语句执行任务的存储模板。您可以通过查询 Information Schema 中的元数据视图 `tasks` 来查看 Task 信息：

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

执行 Task 后会对应生成一个 TaskRun，表示一个 ETL 语句执行任务。TaskRun 有以下状态：

- `PENDING`：任务等待执行。
- `RUNNING`：任务正在执行。
- `FAILED`：任务执行失败。
- `SUCCESS`：任务执行成功。

您可以通过查询 Information Schema 中的元数据视图 `task_runs` 来查看 TaskRun 状态：

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## 相关 FE 参数

您可以通过调整如下 FE 参数配置异步 ETL 任务：

| **参数**                     | **默认值** | **说明**                                                     |
| ---------------------------- | ---------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400      | Task 的有效期，单位秒。超过有效期的 Task 会被自动删除。        |
| task_check_interval_second   | 3600       | 删除过期 Task 的间隔时间，单位秒。                           |
| task_runs_ttl_second         | 86400      | TaskRun 的有效期，单位秒。超过有效期的 TaskRun 会被自动删除。此外，成功和失败状态的 TaskRun 也会被自动删除。 |
| task_runs_concurrency        | 4          | 最多可同时运行的 TaskRun 的数量。                            |
| task_runs_queue_length       | 500        | 最多可同时等待运行的 TaskRun 的数量。如同时等待运行的 TaskRun 的数量超过该参数的默认值，您将无法继续执行 Task。 |
| task_runs_max_history_number | 10000      | 保留的最多历史 TaskRun 任务数量。 |
| task_min_schedule_interval_s | 10         | Task 最小执行间隔。单位：秒。 |

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

示例五：为 INSERT OVERWRITE 语句创建异步任务。任务将以一分钟为间隔定期执行。

```SQL
SUBMIT TASK
SCHEDULE EVERY(INTERVAL 1 MINUTE)
AS
INSERT OVERWRITE insert_wiki_edit
    SELECT dt, user_id, count(*) 
    FROM source_wiki_edit 
    GROUP BY dt, user_id;
```
