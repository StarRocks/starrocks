# ALTER MATERIALIZED VIEW

## Description
You can use `ALTER MATERIALIZED VIEW` to
- Alter the materialzied view's name.
- Alter the materialized view's refresh strategy of an asynchronous materialized view.
- Alter the materialized view's properties.

> **CAUTION**
> - Alter the materialized view's session properties, need add a `session.` prefix to distinguish native materialized view properties, eg: `query_timeout` needs to be `session.query_timeout`, `mv_rewrite_staleness_second` no needs instead.

## Syntax

```SQL
ALTER MATERIALIZED VIEW [database.]mv_name {REFRESH ASYNC new_refresh_scheme_desc | RENAME [database.]new_mv_name | SET (key=string '=' value=string) }
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter**           | **Required** | **Description**                                              |
| ----------------------- | ------------ | ------------------------------------------------------------ |
| mv_name                 | yes          | The name of the materialized view to alter.                  |
| new_refresh_scheme_desc | no           | New async refresh strategy, see [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](../data-definition/CREATE%20MATERIALIZED%20VIEW.md#parameters) for details. |
| new_mv_name             | no           | New name for the materialized view.                          |

## Example

### Example 1: Alter the name of the materialized view

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

### Example 2: Alter the refresh interval of the materialized view

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

### Example 3: Alter the materialized view's properties.

```SQL
-- Change `mv1`'s query_timeout to 4000s
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "40000");
-- Change `mv1`'s mv_rewrite_staleness_second to 600s
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```
