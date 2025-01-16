---
displayed_sidebar: docs
---

# INSERT

## Description

Inserts data into a specific table or overwrites a specific table with data. From v3.2.0 onwards, INSERT supports writing data into files in remote storage. You can use INSERT INTO FILES() to unload data from StarRocks to remote storage.

You can submit an asynchronous INSERT task using [SUBMIT TASK](ETL/SUBMIT_TASK.md).

## Syntax

- **Data loading**:

  ```sql
  INSERT { INTO | OVERWRITE } [db_name.]<table_name>
  [ PARTITION (<partition_name> [, ...] ) ]
  [ TEMPORARY PARTITION (<temporary_partition_name> [, ...] ) ]
  [ WITH LABEL <label>]
  [ (<column_name>[, ...]) | BY NAME ]
  [ PROPERTIES ("key"="value", ...) ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

- **Data unloading**:

  ```sql
  INSERT INTO FILES()
  [ WITH LABEL <label> ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

## Parameters

| Parameter     | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| INTO          | To append data to the table.                                 |
| OVERWRITE     | To overwrite the table with data.                            |
| table_name    | The name of the table into which you want to load data. It can be specified with the database the table resides as `db_name.table_name`. |
| PARTITION    |  The partitions into which you want to load the data. You can specify multiple partitions, which must be separated by commas (,). It must be set to partitions that exist in the destination table. If you specify this parameter, the data will be inserted only into the specified partitions. If you do not specify this parameter, the data will be inserted into all partitions. |
| TEMPORARY PARTITION |The name of the [temporary partition](../../../table_design/data_distribution/Temporary_partition.md) into which you want to load data. You can specify multiple temporary partitions, which must be separated by commas (,).|
| label         | The unique identification label for each data load transaction within the database. If it is not specified, the system automatically generates one for the transaction. We recommend you specify the label for the transaction. Otherwise, you cannot check the transaction status if a connection error occurs and no result is returned. You can check the transaction status via `SHOW LOAD WHERE label="label"` statement. For the naming conventions of labels, see [System Limits](../../System_limit.md). |
| column_name   | The name of the destination column(s) to load data in. It must be set as columns that exist in the destination table. **You cannot specify both `column_name` and `BY NAME`.**<ul><li>If `BY NAME` is not specified, the destination columns you specified are mapped one on one in sequence to the source columns, regardless of what the destination column names are. </li><li>If `BY NAME` is specified, the destination columns are mapped to the source columns with the same names, regardless of the column orders in destination and source tables.</li><li>If no destination column is specified, the default value is all columns in the destination table.</li><li>If the specified column in the source table does not exist in the destination column, the default value will be written to this column. </li><li>If the specified column does not have a default value, the transaction will fail.</li><li>If the column type of the source table is inconsistent with that of the destination table, the system will perform an implicit conversion on the mismatched column.</li><li>If the conversion fails, a syntax parsing error will be returned.</li></ul>**NOTE**<br />From v3.3.1, specifying a column list in the INSERT INTO statement on a Primary Key table will perform Partial Updates (instead of Full Upsert in earlier versions). If the column list is not specified, the system will perform Full Upsert. |
| BY NAME       | To match the source and destination columns by their names. **You cannot specify both `column_name` and `BY NAME`.** If it is not specified, the destination columns are mapped one on one in sequence to the source columns, regardless of what the destination column names are.  |
| PROPERTIES    | Properties of the INSERT job. Each property must be a key-value pair. For supported properties, see [PROPERTIES](#properties). |
| expression    | Expression that assigns values to the column.                |
| DEFAULT       | Assigns default value to the column.                         |
| query         | Query statement whose result will be loaded into the destination table. It can be any SQL statement supported by StarRocks. |
| FILES()       | Table function [FILES()](../../sql-functions/table-functions/files.md). You can use this function to unload data into remote storage. |

### PROPERTIES

INSERT statements support configuring PROPERTIES from v3.4.0 onwards.

| Property         | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| timeout          | The timeout duration of the INSERT job. Unit: Seconds. You can also set the timeout duration for INSERT within the session or globally using the variable `insert_timeout`. |
| strict_mode      | Whether to enable strict mode while loading data using INSERT from FILES(). Valid values: `true` (Default) and `false`. When strict mode is enabled, the system loads only qualified rows. It filters out unqualified rows and returns details about the unqualified rows. For more information, see [Strict mode](../../../loading/load_concept/strict_mode.md). You can also enable strict mode for INSERT from FILES() within the session or globally using the variable `enable_insert_strict`. |
| max_filter_ratio | The maximum error tolerance of INSERT from FILES(). It's the maximum ratio of data records that can be filtered out due to inadequate data quality. When the ratio of unqualified data records reaches this threshold, the job fails. Default value: `0`. Range: [0, 1]. You can also set the maximum error tolerance for INSERT from FILES() within the session or globally using the variable `insert_max_filter_ratio`. |

:::note

- `strict_mode` and `max_filter_ratio` are supported only for INSERT from FILES(). INSERT from tables does not support these properties.
- From v3.4.0 onwards, when `enable_insert_strict` is set to `true`, the system loads only qualified rows. It filters out unqualified rows and returns details about the unqualified rows. Instead, in versions earlier than v3.4.0, when `enable_insert_strict` is set to `true`, the INSERT jobs fails when there is an unqualified rows.

:::

## Return

```Plain
Query OK, 5 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_test', 'status':'VISIBLE', 'txnId':'1008'}
```

| Return        | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| rows affected | Indicates how many rows are loaded. `warnings` indicates the rows that are filtered out. |
| label         | The unique identification label for each data load transaction within the database. It can be assigned by user or automatically by the system. |
| status        | Indicates if the loaded data is visible. `VISIBLE`: the data is successfully loaded and visible. `COMMITTED`: the data is successfully loaded but invisible for now. |
| txnId         | The ID number corresponding to each INSERT transaction.      |

## Usage notes

- As for the current version, when StarRocks executes the INSERT INTO statement, if any row of data mismatches the destination table format (for example, the string is too long), the INSERT transaction fails by default. You can set the session variable `enable_insert_strict` to `false` so that the system filters out the data that mismatches the destination table format and continues to execute the transaction.

- After INSERT OVERWRITE statement is executed, StarRocks creates temporary partitions for the partitions that store the original data, inserts data into the temporary partitions, and swaps the original partitions with the temporary partitions. All these operations are executed in the Leader FE node. Therefore, if the Leader FE node crashes while executing INSERT OVERWRITE statement, the whole load transaction fails, and the temporary partitions are deleted.

### Dynamic Overwrite

From v3.4.0 onwards, StarRocks supports a new semantic - Dynamic Overwrite for INSERT OVERWRITE with partitioned tables.

Currently, the default behavior of INSERT OVERWRITE is as follows:

- When overwriting a partitioned table as a whole (that is, without specifying the PARTITION clause), new data records will replace the data in their corresponding partitions. If there are partitions that are not involved, they will be truncated while the others are overwritten.
- When overwriting an empty partitioned table (that is, with no partitions in it) and specifying the PARTITION clause, the system returns an error `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'`.
- When overwriting a partitioned table and specifying a non-existent partition in the PARTITION clause, the system returns an error `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'`.
- When overwriting a partitioned table with data records that do not match any of the specified partitions in the PARTITION clause, the system either returns an error `ERROR 1064 (HY000): Insert has filtered data in strict mode` (if the strict mode is enabled) or filters the unqualified data records (if the strict mode is disabled).

The behavior of the new Dynamic Overwrite semantic is much different:

When overwriting a partitioned table as a whole, new data records will replace the data in their corresponding partitions. If there are partitions that are not involved, they will be left alone, instead of being truncated or deleted. And if there are new data records correspond to a non-existent partition, the system will create the partition.

The Dynamic Overwrite semantic is disabled by default. To enable it, you need to set the system variable `dynamic_overwrite` to `true`.

Enable Dynamic Overwrite in the current session:

```SQL
SET dynamic_overwrite = true;
```

You can also set it in the hint of the INSERT OVERWRITE statement to allow it take effect for the statement only:.

Example:

```SQL
INSERT OVERWRITE /*+set_var(set dynamic_overwrite = false)*/ insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

## Example

### Example 1: General usage

The following examples are based on table `test`, which contains two columns `c1` and `c2`. The `c2` column has a default value of DEFAULT.

- Import a row of data into the `test` table.

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

When no destination column is specified, the columns are loaded in sequence into the destination table by default. Therefore, in the above example, the outcomes of the first and second SQL statements are the same.

If a destination column (with or without data inserted) uses DEFAULT as the value, the column will use the default value as the loaded data. Therefore, in the above example, the outputs of the third and fourth statements are the same.

- Load multiple rows of data into the `test` table at one time.

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
INSERT INTO test (c1) VALUES (1), (3);
```

Because the results of expressions are equivalent, the outcomes of the first and second statements are the same. The outcomes of the third and fourth statements are the same because they both use default value.

- Import a query statement result into the `test` table.

```SQL
INSERT INTO test SELECT * FROM test2;
INSERT INTO test (c1, c2) SELECT * from test2;
```

- Import a query result into the `test` table, and specify partition and label.

```SQL
INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
```

- Overwrite the `test` table with a query result, and specify partition and label.

```SQL
INSERT OVERWRITE test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test3;
INSERT OVERWRITE test WITH LABEL `label1` (c1, c2) SELECT * from test3;
```

### Example 2: Load Parquet files from AWS S3 using INSERT from FILES()

The following example inserts data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table `insert_wiki_edit`:

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "ap-southeast-1"
);
```

### Example 3: INSERT timeout

The following example inserts the data from the source table `source_wiki_edit` to the target table `insert_wiki_edit` with the timeout duration set to `2` seconds.

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "timeout" = "2"
)
SELECT * FROM source_wiki_edit;
```

If you want to ingest large a dataset, you can set a larger value for `timeout`, or for the session variable `insert_timeout`.

### Example 4: INSERT strict mode and max filter ratio

The following example inserts data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table `insert_wiki_edit`, enables strict mode to filter the unqualified data records, and tolerates at most 10% of error data:

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "strict_mode" = "true",
    "max_filter_ratio" = "0.1"
)
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "XXXXXXXXXX",
    "aws.s3.secret_key" = "YYYYYYYYYY",
    "aws.s3.region" = "us-west-2"
);
```

### Example 5: INSERT match column by name

The following example matches each column in the source and target tables by their names:

```SQL
INSERT INTO insert_wiki_edit BY NAME
SELECT event_time, user, channel FROM source_wiki_edit;
```

In this case, changing the order of `channel` and `user` will not change the column mapping.
