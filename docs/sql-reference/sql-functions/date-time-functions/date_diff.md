# date_diff

## Description

Returns the difference between two DATETIME values in the specified unit. This function returns the value of `datetime1 - datetime2` expressed in terms of `unit`.

This function is supported from v3.1.

## Syntax

```Haskell
BIGINT DATE_DIFF(DATETIME datetime1, DATETIME datetime2, VARCHAR unit)
```

## Parameters

- `datetime1` and `datetime2`: the two datetime values you want to compare, required. Supported data types are DATETIME and DATE.

- `unit`: the unit used to express the time difference, required. The following `unit` values are supported: day, hour, minute, second, millisecond.

## Return value

Returns a BIGINT value.

## Usage notes

- If `datetime1` is earlier than `datetime2`, a negative value is returned.
- If `unit` is invalid, an error is returned.
- If any input value is NULL, NULL is returned.
- If the specified date does not exist, for example, `11-31`, NULL is returned.

## Examples

```Plain Text
mysql> select date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'second');
+-------------------------------------------------------------------+
| date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'second') |
+-------------------------------------------------------------------+
|                                                             10860 |
+-------------------------------------------------------------------+

mysql> select date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'minute');
+------------------------------------------------------------------+
| date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'minute') |
+------------------------------------------------------------------+
|                                                              181 |
+------------------------------------------------------------------+

mysql> select date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'hour');
+----------------------------------------------------------------+
| date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'hour') |
+----------------------------------------------------------------+
|                                                              3 |
+----------------------------------------------------------------+

mysql> select date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'millisecond');
+------------------------------------------------------------------------+
| date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'millisecond') |
+------------------------------------------------------------------------+
|                                                               10860000 |
+------------------------------------------------------------------------+
```

## References

[datediff](./datediff.md)
