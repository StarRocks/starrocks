---
displayed_sidebar: docs
description: "materialized_view_refresh_jobs 提供物化视图刷新的作业级信息。"
---

# materialized_view_refresh_jobs

`materialized_view_refresh_jobs` 提供物化视图刷新的作业级信息。

单次刷新作业可能包含多个 task run（例如逐分区刷新的批次）；该视图将这些 task run 汇总为每个作业一行。它与 [`task_runs`](./task_runs.md) 共用数据源，因此您可以通过 `JOB_ID` 下钻查看某个作业中的各个 task run（`SELECT * FROM information_schema.task_runs WHERE JOB_ID = '<job_id>'`），并且作业记录的保留时长与 `task_runs` 历史记录保持一致。

`materialized_view_refresh_jobs` 提供以下字段：

| 字段                               | 描述                                                         |
| ---------------------------------- | ------------------------------------------------------------ |
| JOB_ID                             | 刷新作业的 ID。同一次刷新的所有 task run 共享该 ID；可用它下钻查看 `task_runs.JOB_ID`。 |
| MATERIALIZED_VIEW_ID               | 物化视图的 ID。                                              |
| TABLE_SCHEMA                       | 物化视图所属的数据库。                                       |
| TABLE_NAME                         | 物化视图的名称。如果物化视图已被删除，则为 `NULL`。         |
| TASK_ID                            | 刷新任务的 ID。                                              |
| WAREHOUSE                          | 刷新作业所使用的 warehouse。                                 |
| RESOURCE_GROUP                     | 刷新作业所使用的资源组。该值为物化视图所配置的 `resource_group` 属性；未配置时返回 `default_mv_wg`。 |
| CREATOR                            | 创建物化视图的用户（即 create-user;运行身份见 RUN_AS_USER）。                         |
| SUBMIT_USER                        | 提交刷新作业的用户。对于手动刷新，该值为发起刷新的用户；对于周期性刷新或基表变更触发的刷新，则由系统提交。 |
| RUN_AS_USER                        | 刷新实际使用的运行用户身份。creator-based 授权（默认，`mv_use_creator_based_authorization=true`）下为物化视图的创建者；root-based 授权下为 `'root'@'%'`。 |
| SUBMIT_TIME                        | 作业提交的时间（第一个 task run 的创建时间）。               |
| REFRESH_STATE                      | 作业的状态，由最后一个 task run 汇总而来。有效值：`PENDING`、`RUNNING`、`FAILED`、`SUCCESS` 和 `SKIPPED`。 |
| FINISH_TIME                        | 作业完成的时间。如果作业尚未完成，则为 `NULL`。             |
| DURATION_TIME                      | 作业的墙钟耗时，单位为秒（最后一个 task run 的完成时间减去第一个 task run 的处理开始时间）。如果作业尚未完成，则为 `NULL`。 |
| REFRESH_TRIGGER                    | 本作业的触发方式。手动执行 `REFRESH MATERIALIZED VIEW` 时为 `MANUAL`（即使物化视图的刷新方案为周期性或自动）；否则为物化视图所配置的刷新方案。有效值：`MANUAL`、`SCHEDULED`、`ON_BASE_TABLE_CHANGE` 和 `NONE`。如果物化视图已被删除且该作业并非手动触发，则为 `UNKNOWN`。 |
| REFRESH_MODE                       | 物化视图所配置的刷新模式。有效值：`PCT` 和 `INCREMENTAL`。如果物化视图已被删除，则为 `NULL`。注意这是**配置值**，不是某次刷新实际采用的模式：bootstrap 首刷以及 INCREMENTAL 规划失败回退 PCT 时，实际模式与本列不同。某次刷新实际采用的模式请查询 `information_schema.task_runs` 的 `get_json_string(EXTRA_MESSAGE, '$.refreshMode')`。 |
| IMV_SOURCE_VERSION_RANGE           | 增量刷新所依据的源版本范围的 JSON，每个基表一个条目。某个范围的 `start` 等于 `end` 表示该基表没有变更，因此「因无变更而跳过」的刷新会为每个基表都报出这样的范围。对于非增量（PCT）刷新，返回 `NULL`。 |
| IMV_SOURCE_TIMESTAMP_RANGE         | 与 IMV_SOURCE_VERSION_RANGE 相同的两个端点所对应的提交时间的 JSON。对于非增量（PCT）刷新返回 `NULL`；增量刷新若端点的提交时间无法解析，同样返回 `NULL`。 |
| IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP  | 固定的源快照 ID 的 JSON，键为基表的 `<catalog>.<db>.<table>` 名称 —— 与 IMV_SOURCE_VERSION_RANGE、IMV_SOURCE_TIMESTAMP_RANGE 所用的键相同，因此三列可按基表关联。在 baseline/PCT 路径刷新时填充；对于纯增量刷新或未固定任何快照时，返回 `NULL`。 |
| FAILED_TASK_RUN_ID                 | 作业中失败 task run 的 ID。如果没有 task run 失败，则为 `NULL`。如需下钻到 `task_runs`，请通过 `FAILED_QUERY_ID = task_runs.QUERY_ID`（或通过 `JOB_ID`）进行 join；`task_runs` 不提供 task-run-id 列。 |
| FAILED_QUERY_ID                    | 失败 task run 的查询 ID。如果没有 task run 失败，则为 `NULL`。 |
| ERROR_CODE                         | 失败 task run 的错误代码。如果没有 task run 失败，则为 `NULL`。 |
| ERROR_MESSAGE                      | 失败 task run 的错误消息。如果没有 task run 失败，则为 `NULL`。 |

:::note
该视图没有持久化存储。其数据行在查询时由 `task_runs` 派生而来，因此记录的保留时长遵循 `task_runs` 历史记录的设置。由于每个作业都是在查询时由其 `task_runs` 数据行汇总而来，因此只有当一个作业的所有 task run 仍处于 `task_runs` 历史记录窗口内时，该作业才会被完整呈现；早于该窗口的作业不会显示，而跨越保留边界的作业可能只被部分汇总（例如，其 `SUBMIT_TIME` 或 `IMV_SOURCE_*` 范围可能仅反映被保留的 task run）。
:::
