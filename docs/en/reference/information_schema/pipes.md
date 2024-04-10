---
displayed_sidebar: "English"
---

# pipes

`pipes` provides information about all pipes stored in the current or specified database. This view is supported from StarRocks v3.2 onwards.

:::note

You can also use [SHOW PIPES](../../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) to view the pipes stored in a specified database or in the current database in use. This command is also supported from v3.2 onwards.

:::

The following fields are provided in `pipes`:

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| DATABASE_NAME | The name of the database in which the pipe is stored.        |
| PIPE_ID       | The unique ID of the pipe.                                   |
| PIPE_NAME     | The name of the pipe.                                        |
| TABLE_NAME    | The name of the destination table. Format: `<database_name>.<table_name>`. |
| STATE         | The status of the pipe. Valid values: `RUNNING`, `FINISHED`, `SUSPENDED`, and `ERROR`. |
| LOAD_STATUS   | The overall status of the data files to be loaded via the pipe, including the following sub-fields:<ul><li>`loadedFiles`: the number of data files that have been loaded.</li><li>`loadedBytes`: the volume of data that has been loaded, measured in bytes.</li><li>`loadingFiles`: the number of data files that are being loaded.</li></ul> |
| LAST_ERROR    | The details about the last error that occurred during the pipe execution. Default value: `NULL`. |
| CREATED_TIME  | The date and time when the pipe was created. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
