# makedate

## Description

Returns a date, given year and day-of-year values.

## Syntax

```Haskell
DATE makedate(INT year, INT dayOfYear);
```

## Parameters

* `year`: Year ranges from 0 to 9999.The supported data type is INT. 

* `dayOfYear`: The second parameter is day of year. In order to maintain the same semantics as the function dayofyear, if this number is greater than 366 or is 366 in a non-leap year, then it is not a day of year
  . The supported data type is INT.

## Return value

Returns the date on the dayOfYear-th day of the given year
day-of-year must be between 1 and the number of days in the current year (365 in normal years, 366 in leap years), otherwise the result is NULL.
The result is also NULL if either argument is NULL.
## Examples

```Plain Text
mysql> select matedate(2023,0);
+-------------------+
| matedate(2023, 0) |
+-------------------+
| NULL              |
+-------------------+

mysql> select matedate(2023,32);
+--------------------+
| matedate(2023, 32) |
+--------------------+
| 2023-02-01         |
+--------------------+

mysql> select matedate(2023,365);
+---------------------+
| matedate(2023, 365) |
+---------------------+
| 2023-12-31          |
+---------------------+

mysql> select matedate(2023,366);
+---------------------+
| matedate(2023, 366) |
+---------------------+
| NULL                |
+---------------------+

mysql> select matedate(9999,365);
+---------------------+
| matedate(9999, 365) |
+---------------------+
| 9999-12-31          |
+---------------------+

mysql> select matedate(10000,1);
+--------------------+
| matedate(10000, 1) |
+--------------------+
| NULL               |
+--------------------+
```

## keyword

MAKEDATE,MAKE
