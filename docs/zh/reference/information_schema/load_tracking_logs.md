---
displayed_sidebar: "Chinese"
---

# load_tracking_logs

提供导入作业相关的错误信息。此视图自 StarRocks v3.0 起支持。

`load_tracking_logs` 提供以下字段：

| **字段**      | **描述**                         |
| ------------- | -------------------------------- |
| JOB_ID        | 导入作业的 ID。                  |
| LABEL         | 导入作业的 Label。               |
| DATABASE_NAME | 导入作业所属的数据库名称。       |
| TRACKING_LOG  | 导入作业的错误日志信息（如有）。 |
