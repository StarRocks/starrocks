---
displayed_sidebar: "English"
---

# SHOW PIPES

## Description

Lists the pipes stored in a specified database or in the current database in use. This command is supported from v3.2 onwards.

## Syntax

```SQL
SHOW PIPES [FROM <db_name>]
[
   WHERE [ NAME { = "<pipe_name>" | LIKE "pipe_matcher" } ]
         [ [AND] STATE = { "SUSPENDED" | "RUNNING" | "ERROR" } ]
]
[ ORDER BY <field_name> [ ASC | DESC ] ]
[ LIMIT { [offset, ] limit | limit OFFSET offset } ]
```

## Parameters

### FROM `<db_name>`

The name of the database for which you want to query pipes. If you do not specify this parameter, the system returns the pipes for the current database in use.

### WHERE

The criteria based on which to query pipes.

### ORDER BY `<field_name>`

The field by which you want to sort the records returned.

### LIMIT

The maximum number of records you want the system to return.

## Return result

The command output consists of the following fields.

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| DATABASE_NAME | The name of the database in which the pipe is stored.        |
| PIPE_ID       | The unique ID of the pipe.                                   |
| PIPE_NAME     | The name of the pipe.                                        |
| TABLE_NAME    | The name of the destination StarRocks table.                 |
| STATE         | The status of the pipe. Valid values: `RUNNING`, `FINISHED`, `SUSPENDED`, and `ERROR`. |
| LOAD_STATUS   | The overall status of the data files to be loaded via the pipe, including the following sub-fields:<ul><li>`loadedFiles`: the number of data files that have been loaded.</li><li>`loadedBytes`: the volume of data that has been loaded, measured in bytes.</li><li>`LastLoadedTime`: the date and time when the last data file was loaded. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`.</li></ul> |
| LAST_ERROR    | The details about the last error that occurred during the pipe execution. Default value: `NULL`. |
| CREATED_TIME  | The date and time when the pipe was created. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |

## Examples

### Query all pipes

Switch to the database named `mydatabase` and show all the pipes in it:

```SQL
USE mydatabase;
SHOW PIPES \G
```

### Query a specified pipe

Switch to the database named `mydatabase` and show the pipe named `user_behavior_replica` in it:

```SQL
USE mydatabase;
SHOW PIPES WHERE NAME = 'user_behavior_replica' \G
```

## References

- [CREATE PIPE](../data-manipulation/CREATE_PIPE.md)
- [ALTER PIPE](../data-manipulation/ALTER_PIPE.md)
- [DROP PIPE](../data-manipulation/DROP_PIPE.md)
- [SUSPEND or RESUME PIPE](../data-manipulation/SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](../data-manipulation/RETRY_FILE.md)
