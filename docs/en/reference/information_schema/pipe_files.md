---
displayed_sidebar: "English"
---

# pipe_files

`pipe_files` provides the status of the data files to be loaded via a specified pipe. This view is supported from StarRocks v3.2 onwards.

The following fields are provided in `pipe_files`:

| **Field**        | **Description**                                              |
| ---------------- | ------------------------------------------------------------ |
| DATABASE_NAME    | The name of the database in which the pipe is stored.        |
| PIPE_ID          | The unique ID of the pipe.                                   |
| PIPE_NAME        | The name of the pipe.                                        |
| FILE_NAME        | The name of the data file.                                   |
| FILE_VERSION     | The digest of the data file.                                 |
| FILE_SIZE        | The size of the data file. Unit: bytes.                      |
| LAST_MODIFIED    | The last time when the data file was modified. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_STATE       | The load status of the data file. Valid values: `UNLOADED`, `LOADING`, `FINISHED`, and `ERROR`. |
| STAGED_TIME      | The date and time when the data file was first recorded by the pipe. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| START_LOAD_TIME  | The date and time when the loading of the data file began. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| FINISH_LOAD_TIME | The date and time when the loading of the data file finished. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| ERROR_MSG        | The details about the load error for the data file.          |
