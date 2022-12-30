# months_add

## Description

Adds specified months to the date, accurate to the month.

## Syntax

```Haskell
DATETIME months_add(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the start time. It must be of the DATETIME or DATE type.

- `expr2`: the months to add. It must be of the INT type, it could be greater, equal or less than zero.

## Return value

Returns a DATETIME value.

## Examples

```Plain
select months_add('2019-08-01 13:21:03', 8);
+--------------------------------------+
| months_add('2019-08-01 13:21:03', 8) |
+--------------------------------------+
| 2020-04-01 13:21:03                  |
+--------------------------------------+

select months_add('2019-08-01', 8);
+-----------------------------+
| months_add('2019-08-01', 8) |
+-----------------------------+
| 2020-04-01 00:00:00         |
+-----------------------------+

select months_add('2019-08-01 13:21:03', -8);
+---------------------------------------+
| months_add('2019-08-01 13:21:03', -8) |
+---------------------------------------+
| 2018-12-01 13:21:03                   |
+---------------------------------------+
```