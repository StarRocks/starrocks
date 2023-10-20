# convert_tz

## Description

Converts a DATE or DATETIME value from one time zone to another.

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/timezone.md).

## Syntax

```Haskell
DATETIME CONVERT_TZ(DATETIME|DATE dt, VARCHAR from_tz, VARCHAR to_tz)
```

## Parameters

- `dt`: the DATE or DATETIME value to convert.

- `from_tz`: the source time zone. VARCHAR is supported. The time zone can be represented in two formats: one is Time Zone Database (for example, Asia/Shanghai) and the other is UTC offset (for example, +08:00).

- `to_tz`: the destination time zone. VARCHAR is supported. Its format is the same as `from_tz`.

## Return value

Returns a value of the DATETIME data type. If the input is a DATE value, it will be converted into a DATETIME value. This function returns NULL if any of the input parameters is invalid or NULL.

## Usage notes

For Time Zone Database, see [List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) (in Wikipedia).

## Examples

Example 1: Convert a datetime in Shanghai to Los_Angeles.

```Plain_Text
select convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles');
+---------------------------------------------------------------------------+
| convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles') |
+---------------------------------------------------------------------------+
| 2019-07-31 22:21:03                                                       |
+---------------------------------------------------------------------------+
1 row in set (0.00 sec)                                                       |
```

Example 2: Convert a date in Shanghai to Los_Angeles.

```Plain_Text
select convert_tz('2019-08-01', 'Asia/Shanghai', 'America/Los_Angeles');
+------------------------------------------------------------------+
| convert_tz('2019-08-01', 'Asia/Shanghai', 'America/Los_Angeles') |
+------------------------------------------------------------------+
| 2019-07-31 09:00:00                                              |
+------------------------------------------------------------------+
1 row in set (0.00 sec)
```

Example 3: Convert a datetime in UTC+08:00 to Los_Angeles.

```Plain_Text
select convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles');
+--------------------------------------------------------------------+
| convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles') |
+--------------------------------------------------------------------+
| 2019-07-31 22:21:03                                                |
+--------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## Keywords

CONVERT_TZ, timezone, time zone
