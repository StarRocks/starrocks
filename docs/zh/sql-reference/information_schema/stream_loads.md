---
displayed_sidebar: docs
---

# stream_loads

`stream_loads` 提供有关 Stream Load 作业的信息。

`stream_loads` 提供以下字段：

| **字段**              | **描述**                                         |
| --------------------- | ------------------------------------------------ |
| LABEL                 | Stream Load 作业的标签。                         |
| ID                    | Stream Load 作业的 ID。                          |
| LOAD_ID               | Stream Load 作业的 Load ID。                     |
| TXN_ID                | Stream Load 作业的事务 ID。                      |
| DB_NAME               | Stream Load 作业所属数据库的名称。               |
| TABLE_NAME            | 数据导入到的表的名称。                           |
| STATE                 | Stream Load 作业的状态。                         |
| ERROR_MSG             | Stream Load 作业失败时的错��消息。               |
| TRACKING_URL          | 用于跟踪 Stream Load 作业的 URL。                |
| CHANNEL_NUM           | Stream Load 作业中使用的通道数量。               |
| PREPARED_CHANNEL_NUM  | Stream Load 作业中已准备的通道数量。             |
| NUM_ROWS_NORMAL       | 导入的正常行数。                                 |
| NUM_ROWS_AB_NORMAL    | 导入的异常行数。                                 |
| NUM_ROWS_UNSELECTED   | 未选择的行数。                                   |
| NUM_LOAD_BYTES        | 导入的字节数。                                   |
| TIMEOUT_SECOND        | Stream Load 作业的超时时间（秒）。               |
| CREATE_TIME_MS        | Stream Load 作业的创建时间（毫秒）。             |
| BEFORE_LOAD_TIME_MS   | 开始导入前的时间（毫秒）。                       |
| START_LOADING_TIME_MS | 开始导入的时间（毫秒）。                         |
| START_PREPARING_TIME_MS | 开始准备的时间（毫秒）。                         |
| FINISH_PREPARING_TIME_MS | 准备完成的时间（毫秒）。                         |
| END_TIME_MS           | Stream Load 作业的结束时间（毫秒）。             |
| CHANNEL_STATE         | 通道的状态。                                     |
| TYPE                  | Stream Load 作业的类型。                         |
| TRACKING_SQL          | 用于跟踪 Stream Load 作业的 SQL 语句。           |
