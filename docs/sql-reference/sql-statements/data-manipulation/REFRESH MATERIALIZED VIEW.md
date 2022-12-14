# REFRESH MATERIALIZED VIEW

## Description

Manually refresh a specific materialized view or partitions within.

> **CAUTION**
>
> You can only manually refresh materialized views that adopt async or manual refresh mode.

## Syntax

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
[PARTITION START ("<partition_start_date>") END ("<partition_end_date>")]
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter**             | **Required** | **Description**                                        |
| ------------------------- | ------------ | ------------------------------------------------------ |
| mv_name                   | yes          | The name of the materialized view to refresh manually. |
| PARTITION START () END () | no           | Manually refresh partitions within a certain time interval. |
| partition_start_date      | no           | The start date of the partitions to refresh manually.  |
| partition_end_date        | no           | The end date of the partitions to refresh manually.    |
| FORCE                     | no           | If you specify this parameter, StarRocks forcibly refreshes the corresponding materialized view or partitions. If you do not specify this parameter, StarRocks automatically judges if a partition is updated and refreshes the partition only when needed. |

## Examples

Example 1: Manually refresh a specific materialized view

```Plain
REFRESH MATERIALIZED VIEW lo_mv1;
```

Example 2: Manually refresh certain partitions of a specific materialized view

```Plain
REFRESH MATERIALIZED VIEW mv 
PARTITION START ("2020-02-01") END ("2020-03-01");
```
