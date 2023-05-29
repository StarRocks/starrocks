# ALTER MATERIALIZED VIEW

## Description

This SQL statement can：

- alter the name of a materialzied view
- alter the refresh strategy of an asynchronous materialized view
- alter the properties of a materialized view

## Syntax

```SQL
ALTER MATERIALIZED VIEW [database.]mv_name { REFRESH ASYNC new_refresh_scheme_desc | RENAME [database.]new_mv_name | SET ( "key" = "value"[,...]) }
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter**           | **Required** | **Description**                                              |
| ----------------------- | ------------ | ------------------------------------------------------------ |
| mv_name                 | yes          | The name of the materialized view to alter.                  |
| new_refresh_scheme_desc | no           | New async refresh strategy, see [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](../data-definition/CREATE%20MATERIALIZED%20VIEW.md#parameters) for details. |
| new_mv_name             | no           | New name for the materialized view.                          |
| key                     | no           | The name of the property to alter, see [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](../data-definition/CREATE%20MATERIALIZED%20VIEW.md#parameters) for details.<br />**NOTE**<br />If you want to alter a session property of the materialized view, you must add a `session.` prefix to the session property, for example, `session.query_timeout`. You do not need to specify the prefix for non-session properties, for example, `mv_rewrite_staleness_second`. |
| value                   | no           | The value of the property to alter, see [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](../data-definition/CREATE%20MATERIALIZED%20VIEW.md#parameters) for details. |

## Example

Example 1: Alter the name of the materialized view

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

Example 2: Alter the refresh interval of the materialized view

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

Example 3: Alter the materialized view's properties.

```SQL
-- Change mv1's query_timeout to 40000 seconds.
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "40000");
-- Change mv1's mv_rewrite_staleness_second to 600 seconds.
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```
