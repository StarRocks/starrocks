---
displayed_sidebar: docs
---

# CREATE PIPE

CREATE PIPE creates a new pipe for defining the INSERT INTO SELECT FROM FILES statement used by the system to load data from a specified source data file to a destination table. This command is supported from v3.2 onwards.

## Syntax

```SQL
CREATE [OR REPLACE] PIPE [db_name.]<pipe_name> 
[PROPERTIES ("<key>" = "<value>"[, "<key> = <value>" ...])]
AS <INSERT_SQL>
```

StarRocks supports CREATE [OR REPLACE] PIPE from v3.2.3 onwards. When you use CREATE [OR REPLACE] PIPE to create a pipe and the pipe name specified in `pipe_name` is the same as the name of an existing pipe in the current database, the existing database is replaced by the new pipe.

## Parameters

### db_name

The unique name of the database to which the pipe belongs.

> **NOTICE**
>
> Each pipe belongs to a specific database. If you drop the database to which a pipe belongs, the pipe is deleted along with the database, and cannot be recovered even if the database is recovered.

### pipe_name

The name of the pipe. The pipe name must be unique within the database in which the pipe is created. For the naming conventions, see [System limits](../../../System_limit.md).

### INSERT_SQL

The INSERT INTO SELECT FROM FILES statement that is used to load data from the specified source data file to the destination table.

For more information about the FILES() table function, see [FILES](../../../sql-functions/table-functions/files.md).

### PROPERTIES

A set of optional parameters that specify how to execute the pipe. Format: `"key" = "value"`.

| Property      | Default value | Description                                                  |
| :------------ | :------------ | :----------------------------------------------------------- |
| AUTO_INGEST   | `TRUE`        | Whether to enable automatic incremental data loads. Valid values: `TRUE` and `FALSE`. If you set this parameter to `TRUE`, automatic incremental data loads are enabled. If you set this parameter to `FALSE`, the system loads only the source data file content specified at job creation and subsequent new or updated file content will not be loaded. For a bulk load, you can set this parameter to `FALSE`. |
| POLL_INTERVAL | `300` second | The polling interval for automatic incremental data loads.   |
| BATCH_SIZE    | `1GB`         | The size of data to be loaded as a batch. If you do not include a unit in the parameter value, the default unit byte is used. |
| BATCH_FILES   | `256`         | The number of source data files to be loaded as a batch.     |

## Examples

Create a pipe named `user_behavior_replica` in the current database to load the data of the sample dataset `s3://starrocks-examples/user_behavior_ten_million_rows.parquet` to the `user_behavior_replica` table:

```SQL
CREATE PIPE user_behavior_replica
PROPERTIES
(
    "AUTO_INGEST" = "TRUE"
)
AS
INSERT INTO user_behavior_replica
SELECT * FROM FILES
(
    "path" = "s3://starrocks-examples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
); 
```

> **NOTE**
>
> Substitute your credentials for `AAA` and `BBB` in the above command. Any valid `aws.s3.access_key` and `aws.s3.secret_key` can be used, as the object is readable by any AWS authenticated user.

This example uses the IAM user-based authentication method and a Parquet file that has the same schema as the StarRocks table. For more information about the other authentication methods and the CREATE PIPE usage, see [Authenticate to AWS resources](../../../../integrations/authenticate_to_aws_resources.md) and [FILES](../../../sql-functions/table-functions/files.md).

## References

- [ALTER PIPE](ALTER_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)
