---
displayed_sidebar: "English"
---

# INSERT

## Description

Inserts data into a specific table or overwrites a specific table with data. For information about the application scenarios, see [Load data with INSERT](../../../loading/InsertInto.md).

You can submit an asynchronous INSERT task using [SUBMIT TASK](./SUBMIT_TASK.md).

## Syntax

```Bash
INSERT { INTO | OVERWRITE } [db_name.]<table_name>
[ PARTITION (<partition_name> [, ...) ]
[ TEMPORARY PARTITION (<temporary_partition_name>[, ...) ]
[ WITH LABEL <label>]
[ (<column_name>[, ...]) ]
{ VALUES ( { <expression> | DEFAULT }[, ...] )
  | <query> }
```

## Parameters

| **Parameter** | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| INTO          | To append data to the table.                                 |
| OVERWRITE     | To overwrite the table with data.                            |
| table_name    | The name of the table into which you want to load data. It can be specified with the database the table resides as `db_name.table_name`. |
| PARTITION    |  The partitions into which you want to load the data. You can specify multiple partitions, which must be separated by commas (,). It must be set to partitions that exist in the destination table. If you specify this parameter, the data will be inserted only into the specified partitions. If you do not specify this parameter, the data will be inserted into all partitions. |
| TEMPORARY PARTITION|The name of the [temporary partition](../../../table_design/Temporary_partition.md) into which you want to load data. You can specify multiple temporary partitions, which must be separated by commas (,).|
| label         | The unique identification label for each data load transaction within the database. If it is not specified, the system automatically generates one for the transaction. We recommend you specify the label for the transaction. Otherwise, you cannot check the transaction status if a connection error occurs and no result is returned. You can check the transaction status via `SHOW LOAD WHERE label="label"` statement. For limitations on naming a label, see [System Limits](../../../reference/System_limit.md). |
| column_name   | The name of the destination column(s) to load data in. It must be set as columns that exist in the destination table. The destination columns you specify are mapped one on one in sequence to the columns of the source table, regardless of what the destination column names are. If no destination column is specified, the default value is all columns in the destination table. If the specified column in the source table does not exist in the destination column, the default value will be written to this column, and the transaction will fail if the specified column does not have a default value. If the column type of the source table is inconsistent with that of the destination table, the system will perform an implicit conversion on the mismatched column. If the conversion fails, a syntax parsing error will be returned. |
| expression    | Expression that assigns values to the column.                |
| DEFAULT       | Assigns default value to the column.                         |
| query         | Query statement whose result will be loaded into the destination table. It can be any SQL statement supported by StarRocks. |

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

## Example

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
