---
displayed_sidebar: docs
---

# be_threads

`be_threads` 提供有关每个 BE 节点上运行的线程的信息。

`be_threads` 提供以下字段：

| **字段**       | **描述**                                         |
| -------------- | ------------------------------------------------ |
| BE_ID          | BE 节点的 ID。                                   |
| GROUP          | 线程组名称。                                     |
| NAME           | 线程名称。                                       |
| PTHREAD_ID     | Pthread ID。                                     |
| TID            | 线程 ID。                                        |
| IDLE           | 指示线程是否空闲（`true`）或不空闲（`false`）。  |
| FINISHED_TASKS | 线程完成的任务数量。                             |
| BOUND_CPUS     | 线程绑定的 CPU。                                 |
