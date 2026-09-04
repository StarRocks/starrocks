---
displayed_sidebar: docs
description: "materialized_views 提供所有物化视图的信息。"
---

# materialized_views

`materialized_views` 提供有关所有物化视图的信息。

`materialized_views` 提供以下字段：

| **字段**                             | **描述**                                         |
| ------------------------------------ | ------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | 物化视图 ID。                                    |
| TABLE_SCHEMA                         | 物化视图所在的数据库名称。                       |
| TABLE_NAME                           | 物化视图名称。                                   |
| REFRESH_TYPE                         | 物化视图（刷新）类型，有效值：`SYNC`（同步物化视图）和 `ASYNC`（异步物化视图，无论以何种方式触发刷新）。当此值为 `SYNC` 时，以下生效状态和刷新相关的字段为空。异步物化视图的刷新方式请参见 `REFRESH_TRIGGER` 和 `REFRESH_POLICY`。 |
| IS_ACTIVE                            | 是否生效，失效的物化视图不会被刷新和查询改写。   |
| INACTIVE_REASON                      | 失效的原因。                                     |
| PARTITION_TYPE                       | 物化视图分区类型。                               |
| TASK_ID                              | 物化视图刷新任务的 ID。                          |
| TASK_NAME                            | 物化视图刷新任务的名称。                         |
| LAST_REFRESH_START_TIME              | 最近一次刷新任务的开始时间。                     |
| LAST_REFRESH_FINISHED_TIME           | 最近一次刷新任务的结束时间。                     |
| LAST_REFRESH_DURATION                | 最近一次刷新的墙钟耗时（秒）：最后一个 task run 的完成时间减去第一个 task run 的开始执行时间。与该 job 在 `materialized_view_refresh_jobs.DURATION_TIME` 中的值一致。 |
| LAST_REFRESH_STATE                   | 最近一次刷新任务的状态。                         |
| LAST_REFRESH_FORCE_REFRESH           | 最近一次刷新任务是否强制刷新。                   |
| LAST_REFRESH_START_PARTITION         | 最近一次刷新任务的开始分区。                     |
| LAST_REFRESH_END_PARTITION           | 最近一次刷新任务的结束分区。                     |
| LAST_REFRESH_BASE_REFRESH_PARTITIONS | 最近一次刷新任务的基表分区。                     |
| LAST_REFRESH_MV_REFRESH_PARTITIONS   | 最近一次刷新任务刷新的分区。                   |
| LAST_REFRESH_ERROR_CODE              | 最近一次刷新任务的错误码。                       |
| LAST_REFRESH_ERROR_MESSAGE           | 最近一次刷新任务的错误信息。                     |
| TABLE_ROWS                           | 物化视图的数据行数，后台统计的近似值。           |
| MATERIALIZED_VIEW_DEFINITION         | 物化视图的 SQL 定义。                            |
| EXTRA_MESSAGE                        | 物化视图的额外信息。                             |
| QUERY_REWRITE_STATUS                 | 物化视图的查询改写状态。                         |
| CREATOR                              | 物化视图的创建者。                               |
| LAST_REFRESH_PROCESS_TIME            | 最近一次刷新任务的处理时间。                     |
| LAST_REFRESH_JOB_ID                  | 最近一次刷新任务的作业 ID。                      |
| LAST_REFRESH_TIME                    | 物化视图已反映基表更新的最新时间。               |
| WAREHOUSE                            | 异步物化视图执行刷新任务所使用的 warehouse 名称。在存算一体模式下，或对于同步（rollup）物化视图，该值为空。 |
| REFRESH_MODE                         | 异步物化视图配置的刷新模式。有效值：`PCT`（分区变更跟踪，仅刷新发生变更的分区）、`INCREMENTAL`（增量视图维护）和 `AUTO`（尽可能增量，遇到无法构建增量计划的变更时回退为 `PCT`）。对于同步物化视图为 NULL。 |
| REFRESH_TRIGGER                      | 刷新的触发方式。有效值：`NONE`（同步物化视图）、`MANUAL`（仅通过 REFRESH MATERIALIZED VIEW 触发）、`SCHEDULED`（周期性触发，通过 EVERY 间隔）和 `ON_BASE_TABLE_CHANGE`（基表导入或变更时自动触发）。 |
| REFRESH_POLICY                       | 可读的刷新策略。有效值：`NONE`、`MANUAL`、`ON_BASE_TABLE_CHANGE`，或形如 `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` 的调度（仅当定义了起始时间时才包含 `START` 子句）。 |
| RESOURCE_GROUP                       | 物化视图刷新任务所使用的资源组（来自物化视图的 `resource_group` 属性）。未设置时默认为 `default_mv_wg`。 |
| QUERY_REWRITE_STATUS_REASON          | `QUERY_REWRITE_STATUS` 的原因。有效值：`OK`、`MV_INACTIVE`、`QUERY_REWRITE_DISABLED`、`UNSUPPORTED_DEFINITION` 和 `UNKNOWN`。 |
| LAST_FRESHNESS_CONFIRMED_AT          | 最近一次成功刷新的开始时间，在整次刷新（其全部 task run）完成后才记录；确认基表无变化、无需刷新的刷新同样会确认新鲜度。物化视图反映该时刻的基表数据。区别于 `LAST_REFRESH_TIME`（基表数据版本时间），这是墙钟时间。首次成功刷新前、以及同步物化视图，为 `NULL`。按分区范围的 REFRESH（部分刷新）不推进该值。 |
| BASE_TABLE_REFRESH_VERSION_TIMES     | 各基表的数据版本时间，以 JSON 对象给出：键为基表的 `catalog.database.table` 名称，值为观测到的最新数据版本时间。这是 `LAST_REFRESH_TIME`（所有基表的单一最大值）背后的按表明细：外部/数据湖基表上报分区源修改时间，OLAP（内部）基表上报可见版本提交时间。无任何基表有可记录时间时为 `{}`。本列仅在刷新成功时推进（失败或跳过的刷新不会改动它）。精度为 1 秒，同一秒内发生的写入与刷新无法区分。 |
| EFFECTIVE_REFRESH_MODE | 该物化视图实际建成的刷新模式。有效值与 `REFRESH_MODE` 相同:`PCT`、`INCREMENTAL`、`AUTO`。它通常与 `REFRESH_MODE` 一致,只有一种情况例外 —— `REFRESH_MODE` 为 `AUTO`,但定义无法增量维护,`CREATE` 于是建成了 `PCT` 物化视图,此列即为 `PCT`。该判定在建表时一次完成且永不改变:只能通过重建物化视图重新尝试增量。同步物化视图为空。 |
| EFFECTIVE_REFRESH_MODE_REASON | `EFFECTIVE_REFRESH_MODE` 与 `REFRESH_MODE` 不同的原因 —— 即该定义无法增量维护的具体说明。建表时记录,之后不再更新。两个模式列一致时为空。 |
| LAST_EXECUTED_REFRESH_MODE | 最近一次刷新实际使用的刷新模式。有效值:`PCT`、`INCREMENTAL`。当 `REFRESH_MODE` 与 `EFFECTIVE_REFRESH_MODE` 都为 `AUTO` 时,此列为 `PCT` 表示只有那一次刷新发生了回退,后续刷新仍可回到增量;而若 `EFFECTIVE_REFRESH_MODE` 为 `PCT`,则表示该物化视图从不尝试增量。因基表无变化而被跳过的刷新不会改变此列的值。首次刷新之前,以及同步物化视图,为空。 |
| LAST_REFRESH_MODE_REASON | 最近一次刷新为什么以 `LAST_EXECUTED_REFRESH_MODE` 的方式执行,而没有走增量刷新。未发生模式降级时为空。取值:`NON_APPEND_ONLY_CHANGE`(基表发生了非 append-only 变更,例如删除分区、清空分区、覆盖写入、外表删除或行级删除)、`BASELINE_UNREACHABLE`(记录的基线已不是表当前 head 的祖先 —— 快照过期，或表被回滚、被替换)、`BASELINE_MISSING`(根本没有可读取增量的基线:首次刷新,或元数据修复之后)、`CHANGE_CAPTURE_DISABLED`(窗口内某个版本是在该基表关闭变更捕获期间产生的)、`FORCE_REFRESH`(强制刷新)、`UNKNOWN`(以上都无法归类的回退;原因在 FE 日志里,不在 `ERROR_MESSAGE` —— 回退本身成功时该字段为空)。 |
| LAST_REFRESH_MODE_REASON_TABLE | 导致该模式决策的基表,格式为 `catalog.database.table`。没有单一基表导致时为空:`FORCE_REFRESH` 源自请求本身而非某张表,由 BE 在读取变更时报出的原因定位到的是 tablet 而非表。 |
