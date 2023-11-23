---
displayed_sidebar: "English"
---

# Information Schema

The StarRocks Information Schema is a database within each StarRocks instance. Information Schema contains several read-only, system-defined views that store extensive metadata information of all objects that the StarRocks instance maintains. The StarRocks Information Schema is based on the SQL-92 ANSI Information Schema, but with the addition of views and functions that are specific to StarRocks.

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

| **View**                                                    | **Description**                                              |
| ----------------------------------------------------------- | ------------------------------------------------------------ |
| [be_bvars](./be_bvars.md)                                       | `be_bvars` provides statistical information regarding bRPC.  |
| [be_cloud_native_compactions](./be_cloud_native_compactions.md) | `be_cloud_native_compactions` provides information on compaction transactions running on CNs (or BEs for v3.0) of a shared-data cluster. |
| [be_compactions](./be_compactions.md)                           | `be_compactions` provides statistical information on compaction tasks. |
| [character_sets](./character_sets.md)                           | `character_sets` identifies the character sets available.    |
| [collations](./collations.md)                                   | `collations` contains the collations available.              |
| [column_privileges](./column_privileges.md)                     | `column_privileges` identifies all privileges granted on columns to a currently enabled role or by a currently enabled role. |
| [columns](./columns.md)                                         | `columns` contains information about all table columns (or view columns). |
| [engines](./engines.md)                                         | `engines` provides information about storage engines.        |
| [events](./events.md)                                           | `events` provides information about Event Manager events.    |
| [global_variables](./global_variables.md)                       | `global_variables` provides information about global variables. |
| [key_column_usage](./key_column_usage.md)                       | `key_column_usage` identifies all columns that are restricted by some unique, primary key, or foreign key constraint. |
| [load_tracking_logs](./load_tracking_logs.md)                   | `load_tracking_logs` provides error information (if any) of load jobs. |
| [loads](./loads.md)                                             | `loads` provides the results of load jobs. Currently, you can only view the results of [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) and [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md) jobs from this view. |
| [materialized_views](./materialized_views.md)                   | `materialized_views` provides information about all asynchronous materialized views. |
| [partitions](./partitions.md)                                   | `partitions` provides information about table partitions.    |
| [pipe_files](./pipe_files.md)                                   | `pipe_files` provides the status of the data files to be loaded via a specified pipe. |
| [pipes](./pipes.md)                                             | `pipes` provides information about all pipes stored in the current or specified database. |
| [referential_constraints](./referential_constraints.md)         | `referential_constraints` contains all referential (foreign key) constraints. |
| [routines](./routines.md)                                       | `routines` contains all stored routines (stored procedures and stored functions). |
| [schema_privileges](./schema_privileges.md)                     | `schema_privileges` provides information about database privileges. |
| [schemata](./schemata.md)                                       | `schemata` provides information about databases.             |
| [session_variables](./session_variables.md)                     | `session_variables` provides information about session variables. |
| [statistics](./statistics.md)                                   | `statistics` provides information about table indexes.       |
| [table_constraints](./table_constraints.md)                     | `table_constraints` describes which tables have constraints. |
| [table_privileges](./table_privileges.md)                       | `table_privileges` provides information about table privileges. |
| [tables](./tables.md)                                           | `tables` provides information about tables.                  |
| [tables_config](./tables_config.md)                             | `tables_config` provides information about the configuration of tables. |
| [task_runs](./task_runs.md)                                     | `task_runs` provides information about the execution of asynchronous tasks. |
| [tasks](./tasks.md)                                             | `tasks` provides information about asynchronous tasks.       |
| [triggers](./triggers.md)                                       | `triggers` provides information about triggers.              |
| [user_privileges](./user_privileges.md)                         | `user_privileges` provides information about user privileges. |
| [views](./views.md)                                             | `views` provides information about all user-defined views.   |

