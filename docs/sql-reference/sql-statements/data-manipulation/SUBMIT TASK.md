# SUBMIT TASK

## Description

Submits an ETL statement as an asynchronous task. This feature has been supported since StarRocks v3.0.

## Syntax

```SQL
SUBMIT TASK [task_name] AS <etl_statement>
```

## Parameters

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| task_name     | The task name.                                               |
| etl_statement | The ETL statement that you want to submit as an asynchronous task. StarRocks currently supports submitting asynchronous tasks for [CREATE TABLE AS SELECT](../data-definition/CREATE%20TABLE%20AS%20SELECT.md) and [INSERT](../data-manipulation/insert.md). |

## Usage notes

You can check the status of asynchronous tasks by querying the metadata table `task_runs` in Information Schema.

```SQL
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Examples

Example 1: Submit an asynchronous task for `CREATE TABLE tbl1 AS SELECT * FROM src_tbl`, and specify the task name as `etl0`:

```SQL
SUBMIT TASK etl0 AS CREATE TABLE tbl1 AS SELECT * FROM src_tbl;
```

Example 2: Submit an asynchronous task for `INSERT INTO tbl2 SELECT * FROM src_tbl`, and specify the task name as `etl1`:

```SQL
SUBMIT TASK etl1 AS INSERT INTO tbl2 SELECT * FROM src_tbl;
```

Example 3: Submit an asynchronous task for `INSERT OVERWRITE tbl3 SELECT * FROM src_tbl`:

```SQL
SUBMIT TASK AS INSERT OVERWRITE tbl3 SELECT * FROM src_tbl;
```

Example 4: Submit an asynchronous task for `INSERT OVERWRITE insert_wiki_edit SELECT * FROM source_wiki_edit`, and extend the query timeout to `100000` seconds using the hint:

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```
