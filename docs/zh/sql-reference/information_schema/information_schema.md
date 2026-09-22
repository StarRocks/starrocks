---
displayed_sidebar: docs
description: "StarRocks Information Schema 是实例中包含系统定义视图的数据库。"
keywords: ['Yuanshuju']
---

# Information Schema

Information Schema 是 StarRocks 实例中的一个数据库。该数据库包含数张由系统定义的视图，这些视图中存储了关于 StarRocks 实例中所有对象的大量元数据信息。

自 v3.2.0 起，Information Schema 支持管理 External Catalog 中的元数据信息。

## 通过 Information Schema 查看元数据信息

您可以通过查询 Information Schema 中的视图来查看 StarRocks 实例中的元数据信息。

以下示例通过查询视图 `tables` 查看 StarRocks 中名为 `table1` 的表相关的元数据信息。

```Plain
MySQL > SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'table1'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: test_db
     TABLE_NAME: table1
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: 
     TABLE_ROWS: 4
 AVG_ROW_LENGTH: 1657
    DATA_LENGTH: 6630
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2023-06-13 11:37:00
    UPDATE_TIME: 2023-06-13 11:38:06
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: 
  TABLE_COMMENT: 
1 row in set (0.01 sec)
```

## Information Schema 中的视图

StarRocks Information Schema 中包含以下视图：

| **视图** | **描述** |
| --- | --- |
| [analyze_status](./analyze_status.md) | 统计信息收集任务的状态。 |
| [applicable_roles](./applicable_roles.md) | 适用于当前用户的角色。 |
| [be_bvars](./be_bvars.md) | bRPC 的统计信息，如部分 StarRocks 组件的 RPC 延迟和 QPS。 |
| [be_cloud_native_compactions](./be_cloud_native_compactions.md) | 存算分离集群的 CN 上运行的 Compaction 事务。 |
| [be_compactions](./be_compactions.md) | Compaction 任务的统计信息。 |
| [be_configs](./be_configs.md) | 每个 BE 节点的配置参数。 |
| [be_logs](./be_logs.md) | 每个 BE 节点的日志。 |
| [be_metrics](./be_metrics.md) | 每个 BE 节点的指标。 |
| [be_tablets](./be_tablets.md) | 每个 BE 节点上的 Tablet。 |
| [be_threads](./be_threads.md) | 每个 BE 节点上运行的线程。 |
| [be_txns](./be_txns.md) | 每个 BE 节点上的事务。 |
| [character_sets](./character_sets.md) | 可用的字符集。 |
| [collations](./collations.md) | 可用的排序规则。 |
| [column_stats_usage](./column_stats_usage.md) | 列统计信息的使用情况。 |
| [columns](./columns.md) | 所有表和视图中的列。 |
| [fe_metrics](./fe_metrics.md) | 每个 FE 节点的指标。 |
| [fe_tablet_schedules](./fe_tablet_schedules.md) | FE 节点上的 Tablet 调度任务。 |
| [fe_threads](./fe_threads.md) | 每个 FE 节点上运行的线程。 |
| [global_variables](./global_variables.md) | 全局变量。 |
| [load_tracking_logs](./load_tracking_logs.md) | 导入作业的错误日志。 |
| [loads](./loads.md) | 导入作业的结果。 |
| [materialized_view_refresh_jobs](./materialized_view_refresh_jobs.md) | 物化视图刷新的作业级信息。 |
| [materialized_views](./materialized_views.md) | 所有物化视图。 |
| [partitions_meta](./partitions_meta.md) | 表的分区。 |
| [pipe_files](./pipe_files.md) | 指定 Pipe 下数据文件的导入状态。 |
| [pipes](./pipes.md) | 当前数据库或指定数据库下的所有 Pipe。 |
| [recyclebin_catalogs](./recyclebin_catalogs.md) | FE 回收站中暂存的已删除数据库、表和分区。 |
| [routine_load_jobs](./routine_load_jobs.md) | Routine Load 作业。 |
| [schemata](./schemata.md) | 数据库。 |
| [session_variables](./session_variables.md) | Session 变量。 |
| [stream_loads](./stream_loads.md) | Stream Load 作业。 |
| [table_bookmarks](./table_bookmarks.md) | OlapTable 当前生效的 Bookmark，包含 `table_bookmark_summary`、`table_bookmark_partitions` 和 `table_bookmark_references` 三张视图。 |
| [tables](./tables.md) | 表。 |
| [tables_config](./tables_config.md) | 表的配置。 |
| [task_runs](./task_runs.md) | 异步任务的执行情况。 |
| [tasks](./tasks.md) | 异步任务。 |
| [verbose_session_variables](./verbose_session_variables.md) | 会话变量的详细信息，包括默认值以及是否已被修改。 |
| [views](./views.md) | 所有用户定义的视图。 |

### 为兼容性而定义的视图

以下视图虽已定义，但在 StarRocks 中并未实现。相应页面中均有说明。

| **视图** | **描述** |
| --- | --- |
| [column_privileges](./column_privileges.md) | 列权限。 |
| [engines](./engines.md) | 存储引擎。 |
| [events](./events.md) | Event Manager 事件。 |
| [key_column_usage](./key_column_usage.md) | 受唯一、主键或外键约束限制的列。 |
| [partitions](./partitions.md) | 表分区。请改用 `partitions_meta`。 |
| [referential_constraints](./referential_constraints.md) | 参照（外键）约束。 |
| [routines](./routines.md) | 存储过程和存储函数。 |
| [schema_privileges](./schema_privileges.md) | 数据库权限。 |
| [statistics](./statistics.md) | 表索引。 |
| [table_constraints](./table_constraints.md) | 具有约束的表。 |
| [table_privileges](./table_privileges.md) | 表权限。 |
| [triggers](./triggers.md) | 触发器。 |
| [user_privileges](./user_privileges.md) | 用户权限。 |
