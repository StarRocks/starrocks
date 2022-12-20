# months_add

## Description

Add the specified month from the date, accurate to the month.

## Syntax

```Haskell
DATETIME months_add(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the start time. It must be of the DATETIME or DATE type.

- `expr2`: the added month. It must be INT type, it could be greater or less than zero.

## Return value

Returns a DATETIME value.

NULL is returned if the date does not exist, for example, 2022-02-29.

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