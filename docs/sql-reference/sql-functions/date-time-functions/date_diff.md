# date_diff

## Description

Returns the difference between two date values in the specified unit. This function returns the value of `expr1 - expr2` expressed in terms of `unit`.

For example, `date_diff('2010-11-30 23:59:59', '2010-11-30 20:58:59', 'second')` returns the difference between the two date values in unit of seconds.

This function is supported from v3.1.

The difference between date_diff and [datediff](./datediff.md) lies in that datediff() does not support the `unit` parameter.

## Syntax

```Haskell
BIGINT DATE_DIFF(DATETIME expr1, DATETIME expr2, VARCHAR unit)
```

## Parameters

- `expr1` and `expr2`: the two datetime values you want to compare, required. Supported data types are DATETIME and DATE.

- `unit`: the unit used to express the time difference, required. The following `unit` values are supported: day, hour, minute, second, millisecond.

## Return value

Returns a BIGINT value.

## Usage notes

- If `expr1` is earlier than `expr2`, a negative value is returned.
- If `unit` is invalid, an error is returned.
- If any input value is NULL, NULL is returned.
- If the specified date does not exist, for example, `2022-11-31`, NULL is returned.

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
