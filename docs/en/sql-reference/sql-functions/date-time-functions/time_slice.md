---
displayed_sidebar: "English"
---

# time_slice

## Description

Converts a given time into the beginning or end of a time interval based on the specified time granularity.

This function is supported from v2.3. v2.5 supports converting a given time into the end of a time interval.

## Syntax

```Haskell
DATETIME time_slice(DATETIME dt, INTERVAL N type[, boundary])
```

## Parameters

- `dt`: the time to convert, DATETIME.
- `INTERVAL N type`: the time granularity, for example, `interval 5 second`.
  - `N` is the length of time interval. It must be an INT value.
  - `type` is the unit, which can be YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND (since 3.1.7), and MICROSECOND (since 3.1.7).
- `boundary`: optional. It is used to specify whether to return the beginning (`FLOOR`) or end (`CEIL`) of the time interval. Valid values: FLOOR, CEIL. If this parameter is not specified, `FLOOR` is the default. This parameter is supported from v2.5.

## Return value

Returns a value of the DATETIME type.

## Usage notes

The time interval starts from A.D. `0001-01-01 00:00:00`.

## Examples

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

Example 1: Convert a given DATETIME value to the beginning of a 5-second time interval without specifying the `boundary` parameter.

```Plaintext
select time_slice(id_datetime, interval 5 second)
from test_all_type_select
order by id_int;
+---------------------------------------------------+
| time_slice(id_datetime, INTERVAL 5 second, floor) |
+---------------------------------------------------+
| 1691-12-23 04:01:05                               |
| 2169-12-18 15:44:30                               |
| 1840-11-23 13:09:50                               |
| 1751-03-21 00:19:00                               |
| 1861-09-12 13:28:15                               |
+---------------------------------------------------+
5 rows in set (0.16 sec)
```

Example 2: Convert a given DATETIME value to the beginning of a 5-day time interval with  `boundary` set to FLOOR.

```Plaintext
select time_slice(id_datetime, interval 5 day, FLOOR)
from test_all_type_select
order by id_int;
+------------------------------------------------+
| time_slice(id_datetime, INTERVAL 5 day, floor) |
+------------------------------------------------+
| 1691-12-22 00:00:00                            |
| 2169-12-16 00:00:00                            |
| 1840-11-21 00:00:00                            |
| 1751-03-18 00:00:00                            |
| 1861-09-12 00:00:00                            |
+------------------------------------------------+
5 rows in set (0.15 sec)
```

Example 3: Convert a given DATETIME value to the end of a 5-day time interval.

```Plaintext
select time_slice(id_datetime, interval 5 day, CEIL)
from test_all_type_select
order by id_int;
+-----------------------------------------------+
| time_slice(id_datetime, INTERVAL 5 day, ceil) |
+-----------------------------------------------+
| 1691-12-27 00:00:00                           |
| 2169-12-21 00:00:00                           |
| 1840-11-26 00:00:00                           |
| 1751-03-23 00:00:00                           |
| 1861-09-17 00:00:00                           |
+-----------------------------------------------+

Example 4: Convert a given DATETIME value to the end of a 1-millisecond time interval.

```Plaintext
select id_datetime, time_slice(id_datetime, interval 1 millisecond, CEIL)
from test_all_type_select
order by id_int;
+---------------------+-------------------------------------------------------+
| id_datetime         | time_slice(id_datetime, INTERVAL 1 millisecond, ceil) |
+---------------------+-------------------------------------------------------+
| 1691-12-23 04:01:09 | 1691-12-23 04:01:09.001000                            |
| 2169-12-18 15:44:31 | 2169-12-18 15:44:31.001000                            |
| 1840-11-23 13:09:50 | 1840-11-23 13:09:50.001000                            |
| 1751-03-21 00:19:04 | 1751-03-21 00:19:04.001000                            |
| 1861-09-12 13:28:18 | 1861-09-12 13:28:18.001000                            |
+---------------------+-------------------------------------------------------+
```

Example 5: Convert a given DATETIME value to the end of a 1-microsecond time interval.

```Plaintext
select id_datetime, time_slice(id_datetime, interval 1 microsecond, CEIL)
from test_all_type_select
order by id_int;
+---------------------+-------------------------------------------------------+
| id_datetime         | time_slice(id_datetime, INTERVAL 1 microsecond, ceil) |
+---------------------+-------------------------------------------------------+
| 1691-12-23 04:01:09 | 1691-12-23 04:01:09.000001                            |
| 2169-12-18 15:44:31 | 2169-12-18 15:44:31.000001                            |
| 1840-11-23 13:09:50 | 1840-11-23 13:09:50.000001                            |
| 1751-03-21 00:19:04 | 1751-03-21 00:19:04.000001                            |
| 1861-09-12 13:28:18 | 1861-09-12 13:28:18.000001                            |
+---------------------+-------------------------------------------------------+
```
