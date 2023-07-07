# INSERT

## Description

Inserts data into a specific table or overwrites a specific table with data. For detailed information about the application scenarios, see [Load data with INSERT](../loading/InsertInto.md).

You can submit an asynchronous INSERT task using [SUBMIT TASK](../data-manipulation/SUBMIT%20TASK.md).

## Syntax

```Bash
INSERT { INTO | OVERWRITE } [db_name.]<table_name>
[ PARTITION (<partition_name> [, ...) ]
[ TEMPORARY_PARTITION (<temporary_partition_name>[, ...) ]
[ WITH LABEL <label>]
[ (<column_name>[, ...]) ]
{ VALUES ( { <expression> | DEFAULT }[, ...] )
  | <query> 
  | TABLE <file_params> <auth_params> }
```

## Parameters

| **Parameter** | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| INTO          | To append data to the table.                                 |
| OVERWRITE     | To overwrite the table with data.                            |
| table_name    | The name of the table into which you want to load data. It can be specified with the database the table resides as `db_name.table_name`. |
| PARTITION    |  The partitions into which you want to load the data. You can specify multiple partitions, which must be separated by commas (,). It must be set to partitions that exist in the target table. If you specify this parameter, the data will be inserted only into the specified partitions. If you do not specify this parameter, the data will be inserted into all partitions. |
| TEMPORARY_PARTITION|The name of the [temporary partition](../../../table_design/Temporary_partition.md) into which you want to load data. You can specify multiple temporary partitions, which must be separated by commas (,).|
| label         | The unique identification label for each data load transaction within the database. If it is not specified, the system automatically generates one for the transaction. We recommend you specify the label for the transaction. Otherwise, you cannot check the transaction status if a connection error occurs and no result is returned. You can check the transaction status via `SHOW LOAD WHERE label="label"` statement. For limitations on naming a label, see [System Limits](../reference/System_limit.md). |
| column_name   | The name of the target column(s) to load data in. It must be set as columns exist in the target table. The order of columns specified in this property must match that of the data source, and the name of fields must match that of the target table. If no target column is specified, the default value is all columns in the target table. If the specified column in the source table does not exist in the target column, the default value will be written to this column, and the transaction will fail if the specified column does not have a default value. If the column type of the source table is inconsistent with that of the target table, the system will perform an implicit conversion on the mismatched column. If the conversion fails, a syntax parsing error will be returned. |
| expression    | Expression that assigns values to the column.                |
| DEFAULT       | Assigns default value to the column.                         |
| query         | Query statement whose result will be loaded into the target table. It can be any SQL statement supported by StarRocks. |
| TABLE         | Inserts data from files in an external data source. Currently, StarRocks supports inserting Parquet and ORC files from AWS S3. This parameter has been supported from v3.1. |
| file_params   | Parameters in the `"key" = "value"` pairs used to define the data file to insert, including: <ul><li>`path`: the URI used to access the file. </li><li>`format`: the format of the file. Valid values include `parquet` and `orc`.</li></ul> |
| auth_params   | Parameters in the `"key" = "value"` pairs used for authentication to access the external data source. <br>AWS S3: <ul><li>`aws.s3.access_key`: The Access Key ID that you can use to access the Amazon S3 bucket. </li><li>`aws.s3.secret_key`: The Secret Access Key that you can use to access the Amazon S3 bucket. </li><li>`aws.s3.region`: The region in which your AWS S3 bucket resides. Example: `us-west-1`.</li></ul> |

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

- As for the current version, when StarRocks executes the INSERT INTO statement, if there is any row of data mismatches the target table format (for example, the string is too long), the INSERT transaction fails by default. You can set the session variable `enable_insert_strict` to `false` so that the system filters out the data that mismatches the target table format and continues to execute the transaction.

- After INSERT OVERWRITE statement is executed, StarRocks creates temporary partitions for the partitions that store the original data, inserts data into the temporary partitions, and swaps the original partitions with the temporary partitions. All these operations are executed in the Leader FE node. Therefore, if the Leader FE node crashes while executing INSERT OVERWRITE statement, the whole load transaction fails, and the temporary partitions are deleted.

## Example

The following examples are based on table `test`, which contains two columns `c1` and `c2`. The `c2` column has a default value of DEFAULT.

- Import a row of data into the`test`table.

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

When no target column is specified, the columns are loaded in sequential order into the target table by default. Therefore, in the above example, the outcomes of the first and second SQL statements are the same.

If there is a target column with no data inserted or data inserted using DEFAULT as the value, the column will use the default value as the loaded data. Therefore, in the above example, the outcomes of the third and fourth statements are the same.

- Load multiple rows of data into the`test`table at one time.

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
