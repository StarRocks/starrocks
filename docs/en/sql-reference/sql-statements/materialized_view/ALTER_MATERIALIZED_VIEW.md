---
displayed_sidebar: docs
---

# ALTER MATERIALIZED VIEW

## Description

This SQL statement can:

- Alter the name of an asynchronous materialized view.
- Alter the refresh strategy of an asynchronous materialized view.
- Alter the status of an asynchronous materialized view to active or inactive.
- Perform an atomic swap between two asynchronous materialized views.
- Alter the properties of an asynchronous materialized view.

  You can use this SQL statement to alter the following properties:

  - `partition_ttl_number`
  - `partition_refresh_number`
  - `resource_group`
  - `auto_refresh_partitions_limit`
  - `excluded_trigger_tables`
  - `mv_rewrite_staleness_second`
  - `unique_constraints`
  - `foreign_key_constraints`
  - `colocate_with`
  - All session variable-related properties. For information on session variables, see [System variables](../../System_variable.md).

:::tip

- This operation requires the ALTER privilege on the target materialized view. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.
- ALTER MATERIALIZED VIEW does not support directly modifying the query statement used to build the materialized view. You can build a new materialized view and swap it with the original one using ALTER MATERIALIZED VIEW SWAP WITH.

:::

## Syntax

```SQL
ALTER MATERIALIZED VIEW [db_name.]<mv_name> 
    { RENAME [db_name.]<new_mv_name> 
    | REFRESH <new_refresh_scheme_desc> 
    | ACTIVE | INACTIVE 
    | SWAP WITH [db_name.]<mv2_name>
    | SET ( "<key>" = "<value>"[,...]) }
```

## Parameters

| **Parameter**           | **Required** | **Description**                                              |
| ----------------------- | ------------ | ------------------------------------------------------------ |
| mv_name                 | yes          | The name of the materialized view to alter.                  |
| new_refresh_scheme_desc | no           | New refresh strategy, see [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](CREATE_MATERIALIZED_VIEW.md#parameters) for details. |
| new_mv_name             | no           | New name for the materialized view.                          |
| ACTIVE                  | no           |Set the status of the materialized view to active. StarRocks automatically sets a materialized view to inactive if any of its base tables is changed, for example, dropped and re-created, to prevent the situation that original metadata mismatches the changed base table. Inactive materialized views cannot be used for query acceleration or query rewrite. You can use this SQL to activate the materialized view after changing the base tables. |
| INACTIVE                | no           | Set the status of the materialized view to inactive. An inactive asynchronous materialized view cannot be refreshed. But you can still query it as a table. |
| SWAP WITH               | no           | Perform an atomic exchange with another asynchronous materialized view after necessary consistency checks. |
| key                     | no           | The name of the property to alter, see [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](CREATE_MATERIALIZED_VIEW.md#parameters) for details.<br />**NOTE**<br />If you want to alter a session variable-related property of the materialized view, you must add a `session.` prefix to the property, for example, `session.query_timeout`. You do not need to specify the prefix for non-session properties, for example, `mv_rewrite_staleness_second`. |
| value                   | no           | The value of the property to alter.                         |

## Example

Example 1: Alter the name of the materialized view.

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

Example 2: Alter the refresh interval of the materialized view.

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

Example 3: Alter the timeout duration for the materialized view refresh tasks to 1 hour (default).

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "3600");
```

Example 4: Alter the materialized view's status to active.

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```

Example 5: Perform an atomic exchange between materialized views `order_mv` and `order_mv1`.

```SQL
ALTER MATERIALIZED VIEW order_mv SWAP WITH order_mv1;
```

Example 6: Enable profile for the materialized view refresh process. This feature is enabled by default.

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_profile" = "true");
```

Example 7: Enable intermediate result spilling for the materialized view refresh process, and set the mode of spilling to `force`. Intermediate result spilling is enabled by default since v3.1.

```SQL
-- Enable spilling during materialized view refresh.
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_spill" = "true");
-- Set spill_mode to force (the default value is auto).
ALTER MATERIALIZED VIEW mv1 SET ("session.spill_mode" = "force");
```

Example 8: Alter the optimizer timeout duration for materialized view to 30 seconds (default since v3.3) if its query statement contains external tables or multiple joins.

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.new_planner_optimize_timeout" = "30000");
```

Example 9: Alter the query rewrite staleness time for the materialized view to 600 seconds.

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```
