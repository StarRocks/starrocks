# RETRY FILE

## Description

Retries to load all data files or a specific data file in a pipe.

## Syntax

```SQL
ALTER PIPE [ IF EXISTS ] <pipe_name> { RETRY ALL | RETRY FILE '<file_name>' }
```

## Parameters

### pipe_name

The name of the pipe.

### file_name

The storage path of the data file that you want to retry to load. Note that you must specify the full storage path of the file. If the file you specify does not belong to the pipe you specify in `pipe_name`, an error is returned.

## Examples

The following example retries to load all data files in a pipe named `user_behavior_replica`:

```SQL
ALTER PIPE [ IF EXISTS ] user_behavior_replica RETRY ALL;
```

The following example retries to load the data file `s3://starrocks-datasets/user_behavior_ten_million_rows.parquet` in a pipe named `user_behavior_replica`:

```SQL
ALTER PIPE [ IF EXISTS ] user_behavior_replica RETRY FILE 's3://starrocks-datasets/user_behavior_ten_million_rows.parquet';
```
