---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER MATERIALIZED VIEW

## 功能

此 SQL 语句可以：

- 变更异步物化视图的名称
- 变更异步物化视图的刷新策略
- 变更异步物化视图的状态为 Active 或 Inactive
- 原子替换异步物化视图
- 变更异步物化视图的属性

  您可以使用此 SQL 语句变更异步物化视图的以下属性：

  - `partition_ttl_number`
  - `partition_refresh_number`
  - `resource_group`
  - `auto_refresh_partitions_limit`
  - `excluded_trigger_tables`
  - `mv_rewrite_staleness_second`
  - `unique_constraints`
  - `foreign_key_constraints`
  - `colocate_with`
  - 所有 Session 变量属性。有关 Session 变量，详细信息请见 [系统变量](../../System_variable.md)。

:::tip
- 该操作需要对应物化视图的 ALTER 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。
- ALTER MATERIALIZED VIEW 不支持直接修改构建物化视图的查询语句。您可以重新构建物化视图，并通过 ALTER MATERIALIZED VIEW SWAP WITH 将其与原始物化视图替换。
:::

## 语法

```SQL
ALTER MATERIALIZED VIEW [db_name.]<mv_name> 
    { RENAME [db_name.]<new_mv_name> 
    | REFRESH <new_refresh_scheme_desc> 
    | ACTIVE | INACTIVE 
    | SWAP WITH [db_name.]<mv2_name>
    | SET ( "<key>" = "<value>"[,...]) }
```

## 参数

| **参数**                | **必选** | **说明**                                                     |
| ----------------------- | -------- | ---------------------------------------------------------- |
| mv_name                 | 是       | 待变更的物化视图的名称。                                       |
| new_refresh_scheme_desc | 否       | 新的刷新机制，详细信息请见 [SQL 参考 - CREATE MATERIALIZED VIEW - 参数](CREATE_MATERIALIZED_VIEW.md#参数)。 |
| new_mv_name             | 否       | 新的物化视图的名称。                                           |
| ACTIVE                  | 否       | 将物化视图的状态设置为 Active。如果物化视图的基表发生更改，例如被删除后重新创建，StarRocks 会自动将该物化视图的状态设置为 Inactive，以避免原始元数据与更改后的基表不匹配的情况。状态为 Inactive 的物化视图无法用于查询加速或改写。更改基表后，您可以使用此 SQL 将该物化视图的状态设置为 Active。 |
| INACTIVE                | 否       | 将物化视图的状态设置为 Inactive。Inactive 状态的物化视图无法被刷新，但您仍然可以将其作为表直接查询。 |
| SWAP WITH               | 否       | 同另一物化视图进行原子替换。替换前，StarRocks 会进行必要的一致性检查。|
| key                     | 否       | 待变更的属性的名称，详细信息请见 [SQL 参考 - CREATE MATERIALIZED VIEW - 参数](CREATE_MATERIALIZED_VIEW.md#参数)。<br />**说明**<br />如需更改物化视图的 Session 变量属性，则必须为 Session 属性添加 `session.` 前缀，例如，`session.query_timeout`。您无需为非 Session 属性指定前缀，例如，`mv_rewrite_staleness_second`。 |
| value                   | 否       | 待变更的属性的值。                                             |

## 示例

示例一：修改物化视图名称

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

示例二：修改物化视图刷新间隔

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

示例三：修改物化视图属性，调整物化视图刷新 Timeout 为一小时（默认）。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "3600");
```

示例四：修改物化视图状态为 Active。

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```

示例五：原子替换物化视图 `order_mv` 和 `order_mv1`。

```SQL
ALTER MATERIALIZED VIEW order_mv SWAP WITH order_mv1;
```

示例六：为物化视图刷新开启 Profile（默认开启）。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_profile" = "true");
```

示例七：为物化视图刷新开启中间结果落盘（自 v3.1 起默认开启），并设置落盘模式为 `force`。

```SQL
-- 为物化视图刷新开启中间结果落盘。
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_spill" = "true");
-- 设置落盘模式为 `force`。
ALTER MATERIALIZED VIEW mv1 SET ("session.spill_mode" = "force");
```

示例八：调整 Optimizer Timeout 为 30 秒（自 v3.3 起为默认值），适用于物化视图查询包含外表或多个 Join。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.new_planner_optimize_timeout" = "30000");
```

示例九：调整物化视图查询改写 Staleness 时间为 600 秒。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```
