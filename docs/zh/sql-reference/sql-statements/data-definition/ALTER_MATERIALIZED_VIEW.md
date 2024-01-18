---
displayed_sidebar: "Chinese"
---

# ALTER MATERIALIZED VIEW

## 功能

修改指定异步物化视图的名称或刷新规则。

此 SQL 语句可以：

- 变更异步物化视图的名称
- 变更异步物化视图的刷新策略
- 变更异步物化视图的状态为 Active 或 Inactive
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
  - 所有 Session 变量属性。有关 Session 变量，详细信息请见 [系统变量](../../../reference/System_variable.md)。

## 语法

```SQL
ALTER MATERIALIZED VIEW [db_name.]<mv_name> 
    { RENAME [db_name.]<new_mv_name> 
    | REFRESH <new_refresh_scheme_desc> 
    | ACTIVE | INACTIVE 
    | SET ( "<key>" = "<value>"[,...]) }
```

## 参数

| **参数**                | **必选** | **说明**                                                     |
| ----------------------- | -------- | ------------------------------------------------------------ |
| mv_name                 | 是       | 待修改属性的物化视图的名称。                                 |
| new_refresh_scheme_desc | 否       | 新的异步刷新规则，详细信息请见 [SQL 参考 - CREATE MATERIALIZED VIEW - 参数](../data-definition/CREATE_MATERIALIZED_VIEW.md#参数)。 |
| new_mv_name             | 否       | 新的物化视图的名称。                                           |
| ACTIVE                  | 否       | 将物化视图的状态设置为 Active。如果物化视图的基表发生更改，例如被删除后重新创建，StarRocks 会自动将该物化视图的状态设置为 Inactive，以避免原始元数据与更改后的基表不匹配的情况。状态为 Inactive 的物化视图无法用于查询加速或改写。更改基表后，您可以使用此 SQL 将该物化视图的状态设置为 Active。 |
| INACTIVE | 否       | 将物化视图的状态设置为 Inactive。Inactive 状态的物化视图无法被刷新，但您仍然可以将其作为表直接查询。 |
| key                     | 否       | 待变更的属性的名称，详细信息请见 [SQL 参考 - CREATE MATERIALIZED VIEW - 参数](../data-definition/CREATE_MATERIALIZED_VIEW.md#参数)。<br />**说明**<br />如需更改物化视图的 Session 变量属性，则必须为 Session 属性添加 `session.` 前缀，例如，`session.query_timeout`。您无需为非 Session 属性指定前缀，例如，`mv_rewrite_staleness_second`。 |
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

示例三：修改物化视图属性

```SQL
-- 修改 mv1 的 query_timeout 为 40000 秒。
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "40000");
-- 修改 mv1 的 mv_rewrite_staleness_second 为 600 秒。
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```

示例四：修改物化视图状态为 Active。

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```
