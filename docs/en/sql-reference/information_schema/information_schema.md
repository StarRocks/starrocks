---
displayed_sidebar: docs
description: "The StarRocks Information Schema is a database within each StarRocks instance."
---

# Information Schema

The StarRocks Information Schema is a database within each StarRocks instance. Information Schema contains several read-only, system-defined views that store extensive metadata information of all objects that the StarRocks instance maintains. The StarRocks Information Schema is based on the SQL-92 ANSI Information Schema, but with the addition of views and functions that are specific to StarRocks.

From v3.2.0, The StarRocks Information Schema supports manage metadata for external catalogs.

## View metadata via Information Schema

You can view the metadata information within a StarRocks instance by querying the content of views in Information Schema.

The following example checks metadata information about a table named `table1` in StarRocks by querying the view `tables`.

```Plain
MySQL > SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'table1'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: test_db
     TABLE_NAME: table1
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: 
     TABLE_ROWS: 4
 AVG_ROW_LENGTH: 1657
    DATA_LENGTH: 6630
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2023-06-13 11:37:00
    UPDATE_TIME: 2023-06-13 11:38:06
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: 
  TABLE_COMMENT: 
1 row in set (0.01 sec)
```

## Views in Information Schema

The StarRocks Information Schema contains the following metadata views:

| **View** | **Description** |
| --- | --- |
| [analyze_status](./analyze_status.md) | Status of analyze jobs. |
| [applicable_roles](./applicable_roles.md) | Roles that are applicable to the current user. |
| [be_bvars](./be_bvars.md) | bRPC statistics, such as RPC latency and QPS, for some StarRocks components. |
| [be_cloud_native_compactions](./be_cloud_native_compactions.md) | Compaction transactions running on the CNs of a shared-data cluster. |
| [be_compactions](./be_compactions.md) | Statistics on compaction tasks. |
| [be_configs](./be_configs.md) | Configuration parameters of each BE node. |
| [be_logs](./be_logs.md) | Logs of each BE node. |
| [be_metrics](./be_metrics.md) | Metrics of each BE node. |
| [be_tablets](./be_tablets.md) | Tablets on each BE node. |
| [be_threads](./be_threads.md) | Threads running on each BE node. |
| [be_txns](./be_txns.md) | Transactions on each BE node. |
| [character_sets](./character_sets.md) | The character sets available. |
| [collations](./collations.md) | The collations available. |
| [column_stats_usage](./column_stats_usage.md) | Usage of column statistics. |
| [columns](./columns.md) | All table columns and view columns. |
| [fe_metrics](./fe_metrics.md) | Metrics of each FE node. |
| [fe_tablet_schedules](./fe_tablet_schedules.md) | Tablet scheduling tasks on FE nodes. |
| [fe_threads](./fe_threads.md) | Threads running on each FE node. |
| [global_variables](./global_variables.md) | Global variables. |
| [load_tracking_logs](./load_tracking_logs.md) | Error logs of load jobs. |
| [loads](./loads.md) | Results of load jobs. |
| [materialized_view_refresh_jobs](./materialized_view_refresh_jobs.md) | Job-level information about materialized view refreshes. |
| [materialized_views](./materialized_views.md) | All materialized views. |
| [partitions_meta](./partitions_meta.md) | Partitions of tables. |
| [pipe_files](./pipe_files.md) | Status of the data files to be loaded via a specified pipe. |
| [pipes](./pipes.md) | All pipes stored in the current or specified database. |
| [recyclebin_catalogs](./recyclebin_catalogs.md) | Deleted databases, tables, and partitions held in the FE recycle bin. |
| [routine_load_jobs](./routine_load_jobs.md) | Routine Load jobs. |
| [schemata](./schemata.md) | Databases. |
| [session_variables](./session_variables.md) | Session variables. |
| [stream_loads](./stream_loads.md) | Stream Load jobs. |
| [table_bookmarks](./table_bookmarks.md) | Active OlapTable bookmarks, in the views `table_bookmark_summary`, `table_bookmark_partitions`, and `table_bookmark_references`. |
| [tables](./tables.md) | Tables. |
| [tables_config](./tables_config.md) | Configuration of tables. |
| [task_runs](./task_runs.md) | Execution of asynchronous tasks. |
| [tasks](./tasks.md) | Asynchronous tasks. |
| [verbose_session_variables](./verbose_session_variables.md) | Session variable details, including default values and whether they have been changed. |
| [views](./views.md) | All user-defined views. |
| [warehouse_metrics](./warehouse_metrics.md) | Metrics of each warehouse. |
| [warehouse_queries](./warehouse_queries.md) | Queries running on each warehouse. |

### Views defined for compatibility

The following views are defined but not implemented in StarRocks. Each page carries a note to that effect.

| **View** | **Description** |
| --- | --- |
| [column_privileges](./column_privileges.md) | Privileges granted on columns. |
| [engines](./engines.md) | Storage engines. |
| [events](./events.md) | Event Manager events. |
| [key_column_usage](./key_column_usage.md) | Columns restricted by a unique, primary key, or foreign key constraint. |
| [partitions](./partitions.md) | Table partitions. Use `partitions_meta` instead. |
| [referential_constraints](./referential_constraints.md) | Referential (foreign key) constraints. |
| [routines](./routines.md) | Stored routines (stored procedures and stored functions). |
| [schema_privileges](./schema_privileges.md) | Database privileges. |
| [statistics](./statistics.md) | Table indexes. |
| [table_constraints](./table_constraints.md) | Which tables have constraints. |
| [table_privileges](./table_privileges.md) | Table privileges. |
| [triggers](./triggers.md) | Triggers. |
| [user_privileges](./user_privileges.md) | User privileges. |
