# microseconds_sub

## Description

Subtracts a time interval from a date value. The time interval is in microseconds.

## Syntax

```Haskell
DATETIME microseconds_sub(DATETIME expr1,INT expr2);
```

## Parameters

`expr1`: the time expression. It must be of the DATETIME type.

`expr2`: the time interval you want to substract, in microseconds. It must be of the INT type.

## Return value

Returns a value of the DATETIME type. If the input value is of the DATE type，the hour, minute, and seconds parts are processed as `00:00:00`.

## Examples

```Plain Text
select microseconds_sub('2010-11-30 23:50:50', 2);
+--------------------------------------------+
| microseconds_sub('2010-11-30 23:50:50', 2) |
+--------------------------------------------+
| 2010-11-30 23:50:49.999998                 |
+--------------------------------------------+

select microseconds_sub('2010-11-30', 2);
+-----------------------------------+
| microseconds_sub('2010-11-30', 2) |
+-----------------------------------+
| 2010-11-29 23:59:59.999998        |
+-----------------------------------+
```
