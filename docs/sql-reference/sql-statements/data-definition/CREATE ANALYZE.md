# CREATE ANALYZE

## Description

Customizes an automatic collection task for collecting CBO statistics.

By default, StarRocks automatically collects full statistics of a table. It checks for any data updates every 5 minutes. If data change is detected, data collection will be automatically triggered. If you do not want to use automatic full collection, you can set the FE configuration item `enable_collect_full_statistic` to `false` and customize a collection task.

Before creating a custom automatic collection task, you must disable automatic full collection (`enable_collect_full_statistic = false`). Otherwise, custom tasks cannot take effect.

This statement is supported from v2.4.

## Syntax

```SQL
-- Automatically collect stats of all databases.
CREATE ANALYZE [FULL|SAMPLE] ALL PROPERTIES (property [,property])

-- Automatically collect stats of all tables in a database.
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
PROPERTIES (property [,property])

-- Automatically collect stats of specified columns in a table.
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
PROPERTIES (property [,property])
```

## Parameter description

- Collection type
  - FULL: indicates full collection.
  - SAMPLE: indicates sampled collection.
  - If no collection type is specified, full collection is used by default.

- `col_name`: columns from which to collect statistics. Separate multiple columns with commas (`,`). If this parameter is not specified, the entire table is collected.

- `PROPERTIES`: custom parameters. If `PROPERTIES` is not specified, the default settings in `fe.conf` are used. The properties that are actually used can be viewed via the `Properties` column in the output of SHOW ANALYZE JOB.

| **PROPERTIES**                        | **Type** | **Default value** | **Description**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | The threshold for determining  whether the statistics for automatic collection are healthy. If the statistics health is below this threshold, automatic collection is triggered. |
| statistics_max_full_collect_data_size | INT      | 100               | The size of the largest partition for automatic collection to collect data. Unit: GB.If a partition exceeds this value, full collection is discarded and sampled collection is performed instead. |
| statistic_sample_collect_rows         | INT      | 200000            | The minimum number of rows to collect.If the parameter value exceeds the actual number of rows in your table, full collection is performed. |

## Examples

Example 1: Automatic full collection

```SQL
-- Automatically collect full stats of all databases.
CREATE ANALYZE ALL;

-- Automatically collect full stats of a database.
CREATE ANALYZE DATABASE db_name;

-- Automatically collect full stats of all tables in a database.
CREATE ANALYZE FULL DATABASE db_name;

-- Automatically collect full stats of specified columns in a table.
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 
```

Example 2: Automatic sampled collection

```SQL
-- Automatically collect stats of all tables in a database using default settings.
CREATE ANALYZE SAMPLE DATABASE db_name;

-- Automatically collect stats of specified columns in a table, with statistics health and the number of rows to collect specified.
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES(
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);
```

## References

[SHOW ANALYZE JOB](../data-definition/SHOW%20ANALYZE%20JOB.md): view the status of a custom collection task.

[DROP ANALYZE](../data-definition/DROP%20ANALYZE.md): delete a custom collection task.

[KILL ANALYZE](../data-definition/KILL%20ANALYZE.md): cancel a custom collection task that is running.

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
