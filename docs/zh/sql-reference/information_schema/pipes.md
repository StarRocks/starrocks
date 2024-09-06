---
displayed_sidebar: docs
---

# pipes

`pipes` 提供当前数据库或指定数据库下所有 Pipe 的详细信息。此视图自 StarRocks v3.2 版本起支持。

:::note

您可以通过 [SHOW PIPES](../../sql-reference/sql-statements/loading_unloading/pipe/SHOW_PIPES.md) 命令查看当前数据库或指定数据库下 Pipe。该命令也是从 3.2 版本开始支持。

:::

`pipes` 提供以下字段：

| **字段**      | **描述**                                                     |
| ------------- | ------------------------------------------------------------ |
| DATABASE_NAME | Pipe 所属数据库的名称。                                      |
| PIPE_ID       | Pipe 的唯一 ID。                                             |
| PIPE_NAME     | Pipe 的名称。                                                |
| TABLE_NAME    | StarRocks 目标表的名称。格式：`<database_name>.<table_name>`。 |
| STATE         | Pipe 的状态，包括 `RUNNING`、`FINISHED`、`SUSPENDED`、`ERROR`。 |
| LOAD_STATUS   | Pipe 下待导入数据文件的整体状态，包括如下字段：<br />`loadedFiles`：已导入的数据文件总个数。<br />`loadedBytes`：已导入的数据总量，单位为字节。<br />`loadingFiles`：正在导入的数据文件总个数。 |
| LAST_ERROR    | Pipe 执行过程中最近一次错误的详细信息。默认值为 `NULL`。     |
| CREATED_TIME  | Pipe 的创建时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
