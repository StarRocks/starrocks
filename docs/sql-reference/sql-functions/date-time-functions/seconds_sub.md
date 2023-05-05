# seconds_sub

## Description

Subtracts a time interval from a date value. The time interval is in seconds.

## Syntax

```Haskell
DATETIME seconds_sub(DATETIME|DATE date, INT seconds);
```

## Parameters

`date`: the time expression. It must be of the DATETIME type.

`seconds`: the time interval you want to substract, in seconds. It must be of the INT type.

## Return value

Returns a value of the DATETIME type.

If the input value is of the DATE type, the hour, minute, and seconds parts are processed as `00:00:00`.

## Examples

```Plain Text
select seconds_sub('2022-01-01 01:01:01', 2);
+---------------------------------------+
| seconds_sub('2022-01-01 01:01:01', 2) |
+---------------------------------------+
| 2022-01-01 01:00:59                   |
+---------------------------------------+

select seconds_sub('2022-01-01 01:01:01', -1);
+----------------------------------------+
| seconds_sub('2022-01-01 01:01:01', -1) |
+----------------------------------------+
| 2022-01-01 01:01:02                    |
+----------------------------------------+

select seconds_sub('2022-01-01', 1);
+------------------------------+
| seconds_sub('2022-01-01', 1) |
+------------------------------+
| 2021-12-31 23:59:59          |
+------------------------------+v
```
