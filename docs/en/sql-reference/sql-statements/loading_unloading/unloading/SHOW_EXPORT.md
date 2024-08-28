---
displayed_sidebar: docs
---

# SHOW EXPORT

## Description

Queries the execution information of export jobs that meet the specified conditions.

## Syntax

```SQL
SHOW EXPORT
[ FROM <db_name> ]
[
WHERE
    [ QUERYID = <query_id> ]
    [ STATE = { "PENDING" | "EXPORTING" | "FINISHED" | "CANCELLED" } ]
]
[ ORDER BY <field_name> [ ASC | DESC ] [, ... ] ]
[ LIMIT <count> ]
```

## Parameters

This statement can contain the following optional clauses:

- FROM

  Specifies the name of the database that you want to query. If you do not specify a FROM clause, StarRocks queries the current database.

- WHERE

  Specifies the conditions based on which you want to filter export jobs. Only the export jobs that meet the specified conditions are returned in the result set of the query.

  | **Parameter** | **Required** | **Description**                                              |
  | ------------- | ------------ | ------------------------------------------------------------ |
  | QUERYID       | No           | The ID of the export job that you want to query. This parameter is used to query the execution information of a single export job. |
  | STATE         | No           | The status of the export jobs that you want to query. Valid values:<ul><li>`PENDING`: specifies to query export jobs that are waiting to be scheduled.</li><li>`EXPORTING`: specifies to query export jobs that are being executed.</li><li>`FINISHED`: specifies to query export jobs that have been successfully completed.</li><li>`CANCELLED`: specifies to query export jobs that have failed.</li></ul> |

- ORDER BY

  Specifies the name of the field based on which you want to sort the export job records in the result set of the query. You can specify multiple fields, which must be separated with commas (`,`). Additionally, you can use the `ASC` or `DESC` keyword to specify that the export job records are sorted in ascending or descending order based on the specified field.

- LIMIT

  Restricts the result set of the query to a specified maximum number of rows. Valid values: positive integer. If you do not specify a LIMIT clause, StarRocks returns all export jobs that meet the specified conditions.

## Return result

For example, query the execution information of an export job whose ID is `edee47f0-abe1-11ec-b9d1-00163e1e238f`:

```SQL
SHOW EXPORT
WHERE QUERYID = "edee47f0-abe1-11ec-b9d1-00163e1e238f";
```

The following execution information is returned:

```SQL
     JobId: 14008
   QueryId: edee47f0-abe1-11ec-b9d1-00163e1e238f
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":["*"],"column separator":"\t","columns":["*"],"tablet num":10,"broker":"","coord num":1,"db":"db0","tbl":"tbl_simple","row delimiter":"\n","mem limit":2147483648}
      Path: hdfs://127.0.0.1:9000/users/230320/
CreateTime: 2023-03-20 11:16:14
 StartTime: 2023-03-20 11:16:17
FinishTime: 2023-03-20 11:16:26
   Timeout: 7200
```

The parameters in the return result are described as follows:

- `JobId`: the ID of the export job.
- `QueryId`: the ID of the query.
- `State`: the status of the export job.

  Valid values:

  - `PENDING`: The export job is waiting to be scheduled.
  - `EXPORTING`: The export job is being executed.
  - `FINISHED`: The export job has been successfully completed.
  - `CANCELLED`: The export job has failed.

- `Progress`: the progress of the export job. The progress is measured in the unit of query plans. Suppose that the export job is divided into 10 query plans and three of them have finished. In this case, the progress of the export job is 30%. For more information, see ["Export data using EXPORT > Workflow"](../../../../unloading/Export.md#workflow).
- `TaskInfo`: the information of the export job.

  The information is a JSON object that consists of the following keys:

  - `partitions`: the partitions on which the exported data resides. If a wildcard (`*`) is returned as a value for this key, the export job is run to export data from all partitions.
  - `column separator`: the column separator used in the exported data file.
  - `columns`: the names of the columns whose data is exported.
  - `tablet num`: the total number of tablets that are exported.
  - `broker`: In v2.4 and earlier, this field is used to return the name of the broker that is used by the export job. From v2.5 onwards, this field returns an empty string. For more information, see ["Export data using EXPORT > Background information"](../../../../unloading/Export.md#background-information).
  - `coord num`: the number of query plans into which the export job is divided.
  - `db`: the name of the database to which the exported data belongs.
  - `tbl`: the name of the table to which the exported data belongs
  - `row delimiter`: the row separator used in the exported data file.
  - `mem limit`: the maximum amount of memory allowed for the export job. Unit: bytes.

- `Path`: the path to which the exported data is stored on the remote storage.
- `CreateTime`: the time when the export job was created.
- `StartTime`: the time when the export job started to be scheduled.
- `FinishTime`: the time when the export job was finished.
- `Timeout`: the amount of time that the export job took than expected. Unit: seconds. The time is counted from `CreateTime`.
- `ErrorMsg`: the reason why the export job throws an error. This field is returned only when the export job encounters an error.

## Examples

- Query all export jobs in the current database:

  ```SQL
  SHOW EXPORT;
  ```

- Query the export job whose ID is `921d8f80-7c9d-11eb-9342-acde48001122` in the database `example_db`:

  ```SQL
  SHOW EXPORT FROM example_db
  WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
  ```

- Query export jobs that are in the `EXPORTING` state in the database `example_db` and specify to sort the export job records in the result set by `StartTime` in ascending order:

  ```SQL
  SHOW EXPORT FROM example_db
  WHERE STATE = "exporting"
  ORDER BY StartTime ASC;
  ```

- Query all export jobs in the database `example_db` and specify to sort the export job records in the result set by `StartTime` in descending order:

  ```SQL
  SHOW EXPORT FROM example_db
  ORDER BY StartTime DESC;
  ```
