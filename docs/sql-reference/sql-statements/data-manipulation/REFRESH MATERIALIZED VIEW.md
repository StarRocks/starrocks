# REFRESH MATERIALIZED VIEW

## Description

Manually refresh a specific materialized view.

> **CAUTION**
>
> You can only manually refresh materialized views that adopt async or manual refresh mode.

## Syntax

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter** | **Required** | **Description**                                        |
| ------------- | ------------ | ------------------------------------------------------ |
| mv_name       | yes          | The name of the materialized view to refresh manually. |

## Examples

### Example 1: Manually refresh a specific materialized view

```Plain
REFRESH MATERIALIZED VIEW lo_mv1;
```
