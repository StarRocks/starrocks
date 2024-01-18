---
displayed_sidebar: "Chinese"
---

# be_cloud_native_compactions

`be_cloud_native_compactions` 提供有关存算一体集群的 CN（或 v3.0 中的 BE）上运行的 Compaction 事务的信息。每个 Compaction 任务会以 Tablet 为粒度，拆分为多个子任务，视图中每一行对应的是一个 Tablet 的 Compaction 子任务。

`be_cloud_native_compactions` 提供以下字段：

| **字段**    | **描述**                                                     |
| ----------- | ------------------------------------------------------------ |
| BE_ID       | CN（BE）的 ID。                                              |
| TXN_ID      | Compaction 任务对应的事务 ID。同一个 Compaction 事务可能会有多个子任务，所以可能存在 TXN_ID 相同的情况。 |
| TABLET_ID   | Compaction 任务对应的 Tablet ID。                            |
| VERSION     | Compaction 任务的输入数据的版本。                            |
| SKIPPED     | 任务是否跳过执行。                                           |
| RUNS        | 任务运行次数，大于 1 表示发生过重试。                        |
| START_TIME  | 开始执行的时间。                                             |
| FINISH_TIME | 结束执行的时间。如果任务还在执行中，值为 NULL。              |
| PROGRESS    | 进度的百分比，范围是 0 到 100。                              |
| STATUS      | 任务运行状态。                                               |

