---
displayed_sidebar: docs
keywords: ['session variable']
description: "StarRocks provides many system variables that can be set and modified to suit your requirements."
---

# System variables

import VariableWarehouse from '../_assets/commonMarkdown/variable_warehouse.mdx'

StarRocks provides many system variables that can be set and modified to suit your requirements. This section describes the variables supported by StarRocks. You can view the settings of these variables by running the [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) command on your MySQL client. You can also use the [SET](sql-statements/cluster-management/config_vars/SET.md) command to dynamically set or modify variables. You can make these variables take effect globally on the entire system, only in the current session, or only in a single query statement.

The variables in StarRocks refer to the variable sets in MySQL, but **some variables are only compatible with the MySQL client protocol and do not function on the MySQL database**.

> **NOTE**
>
> Any user has the privilege to run SHOW VARIABLES and make a variable take effect at session level. However, only users with the SYSTEM-level OPERATE privilege can make a variable take effect globally. Globally effective variables take effect on all the future sessions (excluding the current session).
>
> If you want to make a setting change for the current session and also make that setting change apply to all future sessions, you can make the change twice, once without the `GLOBAL` modifier and once with it. For example:
>
> ```SQL
> SET query_mem_limit = 137438953472; -- Apply to the current session.
> SET GLOBAL query_mem_limit = 137438953472; -- Apply to all future sessions.
> ```

## Variable hierarchy and types

StarRocks supports three types (levels) of variables: global variables, session variables, and `SET_VAR` hints. Their hierarchical relationship is as follows:

* Global variables take effect on global level, and can be overridden by session variables and `SET_VAR` hints.
* Session variables take effect only on the current session, and can be overridden by `SET_VAR` hints.
* `SET_VAR` hints take effect only on the current query statement.

## View variables

You can view all or some variables by using `SHOW VARIABLES [LIKE 'xxx']`. Example:

```SQL
-- Show all variables in the system.
SHOW VARIABLES;

-- Show variables that match a certain pattern.
SHOW VARIABLES LIKE '%time_zone%';
```

## Set variables

### Set variables globally or for a single session

You can set variables to take effect **globally** or **only on the current session**. When set to global, the new value will be used for all the future sessions, while the current session still uses the original value. When set to "current session only", the variable will only take effect on the current session.

A variable set by `SET <var_name> = xxx;` only takes effect for the current session. Example:

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

A variable set by `SET GLOBAL <var_name> = xxx;` takes effect globally. Example:

```SQL
SET GLOBAL query_mem_limit = 137438953472;
```

The following variables only take effect globally. They cannot take effect for a single session, which means you must use `SET GLOBAL <var_name> = xxx;` for these variables. If you try to set such a variable for a single session (`SET <var_name> = xxx;`), an error is returned.

* activate_all_roles_on_login
* character_set_database
* cngroup_low_watermark_cpu_used_permille
* cngroup_low_watermark_running_query_count
* cngroup_resource_usage_fresh_ratio
* cngroup_schedule_mode
* default_rowset_type
* enable_reduce_cast_varchar_expr_sync_type
* enable_reduce_cast_varchar_length_inheritance
* enable_group_level_query_queue
* enable_query_history
* enable_query_queue_load
* enable_query_queue_select
* enable_query_queue_statistic
* enable_table_name_case_insensitive
* enable_tde
* init_connect
* language
* license
* lower_case_table_names
* performance_schema
* query_cache_size
* query_history_keep_seconds
* query_history_load_interval_seconds
* query_queue_concurrency_limit
* query_queue_cpu_used_permille_limit
* query_queue_driver_high_water
* query_queue_driver_low_water
* query_queue_fresh_resource_usage_interval_ms
* query_queue_max_queued_queries
* query_queue_mem_used_pct_limit
* query_queue_pending_timeout_second
* system_time_zone
* version
* version_comment


In addition, variable settings also support constant expressions, such as:

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

 ```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### Set variables in a single query statement

In some scenarios, you may need to set variables specifically for certain queries. By using the `SET_VAR` hint, you can set session variables that will take effect only within a single statement.

StarRocks supports using `SET_VAR` in the following statements;

- SELECT
- INSERT (from v3.1.12 and v3.2.0 onwards)
- UPDATE (from v3.1.12 and v3.2.0 onwards)
- DELETE (from v3.1.12 and v3.2.0 onwards)

`SET_VAR` can only be placed after the above keywords and enclosed in `/*+...*/`.

Example:

```sql
SELECT /*+ SET_VAR(query_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);

UPDATE /*+ SET_VAR(insert_timeout=100) */ tbl SET c1 = 2 WHERE c1 = 1;

DELETE /*+ SET_VAR(query_mem_limit = 8589934592) */
FROM my_table PARTITION p1
WHERE k1 = 3;

INSERT /*+ SET_VAR(insert_timeout = 10000000) */
INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
```

You can also set multiple variables in a single statement. Example:

```sql
SELECT /*+ SET_VAR
  (
  exec_mem_limit = 515396075520,
  query_timeout=10000000,
  batch_size=4096,
  parallel_fragment_exec_instance_num=32
  )
  */ * FROM TABLE;
```

### Set variables as user properties

You can set session variables as user properties using the [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md). This feature is supported from v3.3.3.

Example:

```SQL
-- Set the session variable `query_timeout` to `600` for the user jack.
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## Variable categories

System variables are grouped into the following categories:

- [Query Optimizer and Planning](System_variables/optimizer_planning.md): Session variables that control the cost-based optimizer, statistics-driven planning, pruning, rewrite, and CTE reuse.
- [Joins, Aggregation, and Materialized Views](System_variables/joins_agg_mv.md): Session variables for join execution, aggregation, top-N, and materialized view query rewrite.
- [Execution Engine and Parallelism](System_variables/execution_parallelism.md): Session variables for the pipeline engine, parallelism, runtime filters, and query cache.
- [Memory and Spill](System_variables/memory_spill.md): Session variables that control query memory limits and spilling to disk or remote storage.
- [Scan, I/O, and Data Cache](System_variables/scan_io_datacache.md): Session variables for scan concurrency, I/O tasks, data cache, and page cache.
- [External Catalogs and Data Lake](System_variables/external_catalog_lake.md): Session variables for external catalog connectors and data lake formats such as Hive, Iceberg, and Hudi.
- [Loading, Insert, and Transactions](System_variables/loading_insert_txn.md): Session variables for data loading, INSERT behavior, and transaction handling.
- [Session, Protocol, and Governance](System_variables/session_protocol_governance.md): Session variables for MySQL protocol compatibility, query queue, timeouts, resource groups, and profiling.

## Restricted settings

Some variables are internal and marked with a **Restricted setting** badge on their category page. These generally should not be changed; open a support case or GitHub issue before modifying them.

<VariableWarehouse />
