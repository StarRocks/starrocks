---
displayed_sidebar: docs
---

# fe_tablet_schedules

`fe_tablet_schedules` 提供有关 FE 节点上 Tablet 调度任务的信息。

`fe_tablet_schedules` 提供以下字段：

| **字段**                  | **描述**                               |
| ------------------------- | ---------------------------------------|
| TABLET_ID                 | Tablet 的 ID。                         |
| TABLE_ID                  | Tablet 所属表的 ID。                   |
| PARTITION_ID              | Tablet 所属分区的 ID。                 |
| TYPE                      | 任务的类型。有效值：`REPAIR` 和 `BALANCE`。|
| STATE                     | 任务的状态。                           |
| SCHEDULE_REASON           | 任务调度的原因。                       |
| MEDIUM                    | Tablet 所在的存储介质。                |
| PRIORITY                  | 任务当前的优先级。                     |
| ORIG_PRIORITY             | 任务最初的优先级。                     |
| LAST_PRIORITY_ADJUST_TIME | 上一次任务优先级调整的时间。             |
| VISIBLE_VERSION           | Tablet 可见的数据版本。                |
| COMMITTED_VERSION         | Tablet 已提交的数据版本。              |
| SRC_BE_ID                 | 源副本所在的 BE 的 ID。                |
| SRC_PATH                  | 源副本所在的路径。                     |
| DEST_BE_ID                | 目标副本所在的 BE 的 ID。              |
| DEST_PATH                 | 目标副本所在的路径。                   |
| TIMEOUT                   | 超时时间，默认为 180 秒。              |
| CREATE_TIME               | 任务创建时间。                         |
| SCHEDULE_TIME             | 任务开始调度执行时间。                 |
| FINISH_TIME               | 任务完成时间。                         |
| CLONE_BYTES               | 拷贝文件的大小，单位：字节。            |
| CLONE_DURATION            | 拷贝耗时，单位：秒。                     |
| CLONE_RATE                | 拷贝速率，单位：MB/s。                  |
| FAILED_SCHEDULE_COUNT     | 任务调度失败的次数。                   |
| FAILED_RUNNING_COUNT      | 任务执行失败的次数。                   |
| MSG                       | 任务调度及执行相关的信息。             |
