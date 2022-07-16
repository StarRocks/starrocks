# time_slice

## Description

Converts a given time to the start time of a time interval based on the specified time granularity.

## Syntax

```Plain
DATETIME time_slice(DATETIME dt, INTERVAL N type)
```

## **Parameters**

- `dt`: the time to convert. The supported data type is DATETIME.
- `INTERVAL N type`: the time granularity. `N` is a number of the INT type. `type` is the unit, which can be YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, WEEK, and QUARTER.

## Return value

Returns a value of the DATETIME type.

## Usage notes

The time interval starts from A.D. `0001-01-01 00:00:00`.

## Examples

Example 1: Convert a given time to the start time of a 5-second time interval.

In this example, the time granularity is 5 second. The first time interval is from `2022-04-26 19:01:00` to `2022-04-26 19:01:04`. `2022-04-26 19:01:07` falls into the second interval which starts from `2022-04-26 19:01:05` and this value is returned.

```Plain
mysql> select time_slice('2022-04-26 19:01:07', interval 5 second);
+------------------------------------------------------+
| time_slice('2022-04-26 19:01:07', INTERVAL 5 SECOND) |
+------------------------------------------------------+
| 2022-04-26 19:01:05                                  |
+------------------------------------------------------+
```

Example 2: Convert a given time to the start time of a 5-day time interval.

In this example, the time granularity is 5 day. The first time interval is from `0001-01-01 19:01:07` to `0001-01-05 19:01:07`. `0001-01-07 19:01:07` falls into the second interval which starts from `0001-01-06 19:01:07` and this value is returned.

```Plain
mysql> select time_slice('0001-01-07 19:01:07', interval 5 day);
+---------------------------------------------------+
| time_slice('0001-01-07 19:01:07', INTERVAL 5 DAY) |
+---------------------------------------------------+
| 0001-01-06 00:00:00                               |
+---------------------------------------------------+
```
