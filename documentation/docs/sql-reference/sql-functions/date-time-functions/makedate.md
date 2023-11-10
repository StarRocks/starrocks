# makedate

## Description

Creates and returns a date based on the given year and day of year values.

This function is supported from v3.1.

## Syntax

```Haskell
DATE makedate(INT year, INT dayOfYear);
```

## Parameters

- `year`: It ranges from 0 to 9999. NULL is returned if this range is exceeded. The supported data type is INT.
- `dayOfYear`: the day of year. The supported data type is INT. In order to maintain the same semantics as the function [dayofyear](./dayofyear.md), if this number is greater than 366 or is 366 in a non-leap year, it is not a day of year.

## Return value

Returns the date on the dayOfYear-th day of the given year.

- `year` must be in the range of [0,9999]. Otherwise, NULL is returned.
- `dayOfYear` must be between 1 and the number of days in the current year (365 in normal years, 366 in leap years). Otherwise, NULL is returned.
- The result is also NULL if either input parameter is NULL.

## Examples

```Plain Text
mysql> select makedate(2023,0);
+-------------------+
| makedate(2023, 0) |
+-------------------+
| NULL              |
+-------------------+

mysql> select makedate(2023,32);
+--------------------+
| makedate(2023, 32) |
+--------------------+
| 2023-02-01         |
+--------------------+

mysql> select makedate(2023,365);
+---------------------+
| makedate(2023, 365) |
+---------------------+
| 2023-12-31          |
+---------------------+

mysql> select makedate(2023,366);
+---------------------+
| makedate(2023, 366) |
+---------------------+
| NULL                |
+---------------------+

mysql> select makedate(9999,365);
+---------------------+
| makedate(9999, 365) |
+---------------------+
| 9999-12-31          |
+---------------------+

mysql> select makedate(10000,1);
+--------------------+
| makedate(10000, 1) |
+--------------------+
| NULL               |
+--------------------+
```

## keyword

MAKEDATE,MAKE
