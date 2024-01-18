---
displayed_sidebar: "English"
---

# date_slice

## Description

Converts a given time into the beginning or end of a time interval based on the specified time granularity.

This function is supported from v2.5.

## Syntax

```Haskell
DATE time_slice(DATE dt, INTERVAL N type[, boundary])
```

## Parameters

- `dt`: the time to convert, DATE.
- `INTERVAL N type`: the time granularity, for example, `interval 5 day`.
  - `N` is the length of time interval. It must be an INT value.
  - `type` is the unit, which can be YEAR, QUARTER, MONTH, WEEK, DAY.  If `type` is set to HOUR, MINUTE, or SECOND for a DATE value, an error is returned.
- `boundary`: optional. It is used to specify whether to return the beginning (`FLOOR`) or end (`CEIL`) of the time interval. Valid values: FLOOR, CEIL. If this parameter is not specified, `FLOOR` is the default.

## Return value

Returns a value of the DATE type.

## Usage notes

The time interval starts from A.D. `0001-01-01 00:00:00`.

## Examples

Example 1: Convert a given time to the beginning of a 5-year time interval without specifying the `boundary` parameter.

```Plaintext
select date_slice('2022-04-26', interval 5 year);
+--------------------------------------------------+
| date_slice('2022-04-26', INTERVAL 5 year, floor) |
+--------------------------------------------------+
| 2021-01-01                                       |
+--------------------------------------------------+
```

Example 2: Convert a given time to the end of a 5-day time interval.

```Plaintext
select date_slice('0001-01-07', interval 5 day, CEIL);
+------------------------------------------------+
| date_slice('0001-01-07', INTERVAL 5 day, ceil) |
+------------------------------------------------+
| 0001-01-11                                     |
+------------------------------------------------+
```

The following examples are provided based on the `test_all_type_select` table.

```Plaintext
select * from test_all_type_select order by id_int;
+------------+---------------------+--------+
| id_date    | id_datetime         | id_int |
+------------+---------------------+--------+
| 2052-12-26 | 1691-12-23 04:01:09 |      0 |
| 2168-08-05 | 2169-12-18 15:44:31 |      1 |
| 1737-02-06 | 1840-11-23 13:09:50 |      2 |
| 2245-10-01 | 1751-03-21 00:19:04 |      3 |
| 1889-10-27 | 1861-09-12 13:28:18 |      4 |
+------------+---------------------+--------+
5 rows in set (0.06 sec)
```

Example 3: Convert a given DATE value to the beginning of a 5-second time interval.

```Plaintext
select date_slice(id_date, interval 5 second, FLOOR)
from test_all_type_select
order by id_int;
ERROR 1064 (HY000): can't use date_slice for date with time(hour/minute/second)
```

An error is returned because the system cannot find the second part of a DATE value.

Example 4: Convert a given DATE value to the beginning of a 5-day time interval.

```Plaintext
select date_slice(id_date, interval 5 day, FLOOR)
from test_all_type_select
order by id_int;
+--------------------------------------------+
| date_slice(id_date, INTERVAL 5 day, floor) |
+--------------------------------------------+
| 2052-12-24                                 |
| 2168-08-03                                 |
| 1737-02-04                                 |
| 2245-09-29                                 |
| 1889-10-25                                 |
+--------------------------------------------+
5 rows in set (0.14 sec)
```

Example 5: Convert a given DATE value to the end of a 5-day time interval.

```Plaintext
select date_slice(id_date, interval 5 day, CEIL)
from test_all_type_select
order by id_int;
+-------------------------------------------+
| date_slice(id_date, INTERVAL 5 day, ceil) |
+-------------------------------------------+
| 2052-12-29                                |
| 2168-08-08                                |
| 1737-02-09                                |
| 2245-10-04                                |
| 1889-10-30                                |
+-------------------------------------------+
5 rows in set (0.17 sec)
```
