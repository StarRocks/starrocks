# StarRocks 3.2

## 3.2.0 - rc01

Release date: November xx, 2023

### New Features

#### Shared-data cluster

- Supports the persistent index for Primary Key tables on local disks.

#### Data Lake Analytics

- Supports creating and dropping databases and managed tables in Hive catalogs, and supports exporting data to Hive's managed tables using INSERT or INSERT OVERWRITE.
- Supports Unified Catalog, with which users can access different table formats (Hive, Iceberg, Hudi, and Delta Lake) that share a common metastore like Hive metastore or AWS Glue.

#### Storage engine, data ingestion, and export

- Added the following features of loading with the table function [FILES()](../sql-reference/sql-functions/table-functions/files.md):
  - Loading Parquet and ORC format data from Azure or GCP.
  - Extracting the value of a key/value pair from the file path as the value of a column using the parameter `columns_from_path`.
  - Automatic table schema detection and unionization of the same batch of data files. It is useful when loading data files from Hive when these data files are stored under different storage paths and have different table schemas.
  - Loading complex data types including ARRAY, JSON, MAP, and STRUCT.
- Supports the dict_mapping column property, which can significantly facilitate the loading process during the construction of a global dictionary.
- Supports unloading data from StarRocks to Parquet-formatted files stored in AWS S3 or HDFS by using INSERT INTO FILES. For detailed instructions, see [Unload data using INSERT INTO FILES](../unloading/unload_using_insert_into_files.md).

#### SQL reference

Added the following functions:

- String functions: substring_index, url_extract_parameter, url_encode, url_decode, and translate
- Date functions: dayofweek_iso, week_iso, quarters_add, quarters_sub, milliseconds_add, milliseconds_sub, date_diff, jodatime_format, str_to_jodatime, to_iso8601, to_tera_date, and to_tera_timestamp
- Pattern matching function: regexp_extract_all
- hash function: xx_hash3_64
- Aggregate functions: approx_top_k
- Window functions: cume_dist, percent_rank and session_number
- Utility functions: dict_mapping and get_query_profile

### Improvements

#### Materialized View

Asynchronous materialized view

- Creation:

  Supports automatic refresh for an asynchronous materialized view created upon views or materialized views when schema changes occur on the views, materialized views, or their base tables.

- Observability:

  Supports Query Dump for asynchronous materialized views.

- The Spill to Disk feature is enabled by default for the refresh tasks of asynchronous materialized views, reducing memory consumption.
- Data consistency:

  - Added the property `query_rewrite_consistency` for asynchronous materialized view creation. This property defines the query rewrite rules based on the consistency check.
  - Add the property `force_external_table_query_rewrite` for external catalog-based asynchronous materialized view creation. This property defines whether to allow force query rewrite for asynchronous materialized views created upon external catalogs.

  For detailed information, see [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md).

- Added a consistency check for materialized views' partitioning key.

  When users create an asynchronous materialized view with window functions that include a PARTITION BY expression, the partitioning column of the window function must match that of the materialized view.

#### Storage engine, data ingestion, and export

- Optimized the persistent index for Primary Key tables by improving memory usage logic while reducing I/O read and write amplification. [#24875](https://github.com/StarRocks/starrocks/pull/24875)  [#27577](https://github.com/StarRocks/starrocks/pull/27577)  [#28769](https://github.com/StarRocks/starrocks/pull/28769)
- Supports data re-distribution across local disks for Primary Key tables.
- Partitioned tables support automatic cooldown based on the partition time range and cooldown time.
- The Publish phase of a load job that writes data into a Primary Key table is changed from asynchronous mode to synchronous mode. As such, the data loaded can be queried immediately after the load job finishes.

#### Query

- Optimized StarRocks' compatibility with Metabase and Superset. Supports integrating them with external catalogs.

#### SQL Reference

- array_agg supports the keyword DISTINCT.

### Developer tools

- Supports Trace Query Profile for asynchronous materialized views, which can be used to analyze its transparent rewrite.

### Compatibility Changes

#### Behavior Changes

To be updated.

#### Parameters

- Added new parameters for Data Cache.

#### System Variables

To be updated.

### Bug Fixes

Fixed the following issues:

- BEs crash when libcurl is invoked. [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- Schema Change may fail if it takes an excessive period of time, because the specified tablet version is handled by garbage collection. [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- Failed to access the Parquet files in MinIO or AWS S3 via file external tables. [#29873] (https://github.com/StarRocks/starrocks/pull/29873)
- The ARRAY, MAP, and STRUCT type columns are not correctly displayed in  `information_schema.columns`. [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- An error is reported if specific path formats are used during data loading via Broker Load: `msg:Fail to parse columnsFromPath, expected: [rec_dt]`. [#32720](https://github.com/StarRocks/starrocks/pull/32720)
- `DATA_TYPE` and `COLUMN_TYPE` for BINARY or VARBINARY data types are displayed as `unknown` in the `information_schema.columns` view. [#32678](https://github.com/StarRocks/starrocks/pull/32678)

### Upgrade Notes

To be updated.
