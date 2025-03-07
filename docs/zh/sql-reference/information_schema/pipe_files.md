---
displayed_sidebar: docs
---

# pipe_files

`pipe_files` 提供指定 Pipe 下数据文件的导入状态。此视图自 StarRocks v3.2 版本起支持。

`pipe_files` 提供以下字段：

| **字段**         | **描述**                                                     |
| ---------------- | ------------------------------------------------------------ |
| DATABASE_NAME    | Pipe 所属数据库的名称。                                      |
| PIPE_ID          | Pipe 的唯一 ID。                                             |
| PIPE_NAME        | Pipe 的名称。                                                |
| FILE_NAME        | 数据文件的名称。                                             |
| FILE_VERSION     | 数据文件的内容摘要值。                                       |
| FILE_SIZE        | 数据文件的大小。单位：字节。                                 |
| LAST_MODIFIED    | 数据文件的最后修改时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| LOAD_STATE       | 数据文件的导入状态，包括 `UNLOADED`、`LOADING`、`FINISHED`、`ERROR`。 |
| STAGED_TIME      | 数据文件被 Pipe 首次记录的时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| START_LOAD_TIME  | 数据文件导入开始的时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| FINISH_LOAD_TIME | 数据文件导入结束的时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。 |
| ERROR_MSG        | 数据文件的导入错误信息。                                     |
