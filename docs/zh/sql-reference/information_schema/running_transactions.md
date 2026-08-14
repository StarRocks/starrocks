---
displayed_sidebar: docs
description: "running_transactions 列出所有数据库中当前正在运行的事务。"
---

# running_transactions

`running_transactions` 为所有数据库中每一个当前正在运行（即处于非终态）的事务返回一行。一旦事务到达终态（`VISIBLE` 或 `ABORTED`），它就会从该视图中消失。

视图中的行涵盖各类正在运行的事务，包括普通导入事务、Routine Load 任务以及湖仓（lake）Compaction 事务。

`running_transactions` 提供以下字段：

| 字段                  | 描述                                                         |
| --------------------- | ------------------------------------------------------------ |
| TXN_ID                | 事务 ID。                                                    |
| GLOBAL_TXN_ID         | 全局事务 ID（GTID）。如果事务没有 GTID，则为 `0`。            |
| LABEL                 | 事务标签（Label）。                                          |
| DATABASE_ID           | 事务所属数据库的 ID。                                        |
| DATABASE_NAME         | 数据库名称。 |
| TABLE_IDS             | 事务涉及的表 ID，以逗号连接。                                |
| TABLE_NAMES           | 事务涉及的表名，以逗号连接。尽力而为：无法解析为表名的 ID 会以原始 ID 显示。 |
| STATE                 | 事务状态。有效值：<ul><li>`PREPARE`：事务已开始。</li><li>`PREPARED`：事务已预提交。</li><li>`COMMITTED`：事务已提交，等待发布为 `VISIBLE`。</li></ul> |
| COORDINATOR           | 事务的协调节点，例如 `FE: 127.0.0.1`。                       |
| SOURCE_TYPE           | 事务的导入来源类型，例如 `BACKEND_STREAMING`、`INSERT_STREAMING`、`LAKE_COMPACTION`、`ROUTINE_LOAD_TASK` 或 `FRONTEND`。 |
| WAREHOUSE_ID          | 事务所属仓库（Warehouse）的 ID。                            |
| PREPARE_TIME          | 事务开始（进入 `PREPARE`）的时间。未设置时为 `NULL`。        |
| PREPARED_TIME         | 事务进入 `PREPARED` 的时间。未设置时为 `NULL`。              |
| COMMIT_TIME           | 事务提交的时间。尚未提交时为 `NULL`。                        |
| PUBLISH_TIME          | 发布开始的时间。尚未开始发布时为 `NULL`。                    |
| FINISH_TIME           | 事务完成的时间。对于正在运行的事务始终为 `NULL`，因为已完成的事务会从该视图中消失。 |
| PENDING_PUBLISH_MS    | 对于 `COMMITTED` 状态的事务，表示其等待发布为 `VISIBLE` 的时长（毫秒，即当前时间减去提交时间）；其他状态下为 `0`。这是诊断发布卡住问题的核心字段。 |
| TIMEOUT_MS            | 事务超时时间（毫秒）。                                       |
| PREPARED_TIMEOUT_MS   | `PREPARED` 状态的超时时间（毫秒）。                          |
| ERROR_REPLICA_NUM     | 错误副本数量。                                               |
| REASON                | 中止或失败原因文本，可能为空。                               |
| ERROR_MSG             | 错误信息文本，可能为空。                                     |
| IS_NO_OP_PUBLISH      | 该发布是否为空操作（no-op）。                                |
| NO_OP_PUBLISH_REASON  | 发布为空操作的原因。                                         |

## 使用说明

`running_transactions` 是用于诊断发布卡住（publish stall）问题的一个观测视图，该问题在存算分离（shared-data，湖仓）模式下最为明显。当版本发布卡住时，事务会堆积在 `COMMITTED` 状态，而 `PENDING_PUBLISH_MS` 显示每个事务等待发布为 `VISIBLE` 的时长。按 `PENDING_PUBLISH_MS` 对 `COMMITTED` 事务排序，可优先显示等待最久、卡住最严重的事务。

由于正在运行的事务集合仅在 FE Leader 上是权威的，因此对该视图的扫描始终由 Leader FE 提供服务。

`running_transactions` 会按查询用户的权限过滤行：仅当用户对某个数据库、或其中的任意对象拥有任意权限（例如对该数据库本身拥有权限，或对其中某张表拥有 `SELECT` 权限）时，才会显示该数据库上的事务。拥有集群级访问权限的用户（例如 `root` 用户或具有广泛授权的用户）可以看到所有正在运行的事务。如果某事务所属的数据库在其运行期间被删除，则该事务无法完成鉴权，会对所有用户（包括管理员）隐藏。

:::note

请勿假设该视图的 `COUNT(*)` 等于 `txn_running` 指标。二者的计算方式不同，可能并不一致。

:::

## 示例

查找等待发布时间最长的已提交事务：

```sql
SELECT TXN_ID, DATABASE_NAME, TABLE_NAMES, STATE, PENDING_PUBLISH_MS
FROM information_schema.running_transactions
WHERE STATE = 'COMMITTED'
ORDER BY PENDING_PUBLISH_MS DESC;
```
