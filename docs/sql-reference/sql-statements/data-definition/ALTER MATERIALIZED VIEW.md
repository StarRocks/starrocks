# ALTER MATERIALIZED VIEW

## Description

Alters the name or refresh strategy of an asynchronous materialized view.

## Syntax

```SQL
ALTER MATERIALIZED VIEW [database.]mv_name {REFRESH ASYNC new_refresh_scheme_desc | RENAME [database.]new_mv_name}
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
