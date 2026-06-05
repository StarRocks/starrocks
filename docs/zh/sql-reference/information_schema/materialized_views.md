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
| LAST_REFRESH_DURATION                | 最近一次刷新任务的持续时间。                     |
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
| REFRESH_MODE                         | 异步物化视图配置的刷新模式。有效值：`PCT`（分区变更跟踪，仅刷新发生变更的分区）、`INCREMENTAL`（增量视图维护）和 `AUTO`。对于同步物化视图为空。 |
| REFRESH_TRIGGER                      | 刷新的触发方式。有效值：`NONE`（同步物化视图）、`MANUAL`（仅通过 REFRESH MATERIALIZED VIEW 触发）、`SCHEDULED`（周期性触发，通过 EVERY 间隔）和 `ON_BASE_TABLE_CHANGE`（基表导入或变更时自动触发）。 |
| REFRESH_POLICY                       | 可读的刷新策略。有效值：`NONE`、`MANUAL`、`ON_BASE_TABLE_CHANGE`，或形如 `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` 的调度（仅当定义了起始时间时才包含 `START` 子句）。 |
| RESOURCE_GROUP                       | 物化视图刷新任务所使用的资源组（来自物化视图的 `resource_group` 属性）。未设置时默认为 `default_mv_wg`。 |