---
displayed_sidebar: docs
---

# months_add

## Description

Adds a specified number of months to the date, accurate to the month.

The [add_months](./add_months.md) function provides similar functionalities.

## Syntax

```Haskell
DATETIME months_add(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the start time. It must be of the DATETIME or DATE type.

- `expr2`: the months to add. It must be of the INT type. It can be greater, equal, or less than zero. A negative value subtracts months from `date`.

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

select months_add('2019-02-28 13:21:03', 1);
+--------------------------------------+
| months_add('2019-02-28 13:21:03', 1) |
+--------------------------------------+
| 2019-03-28 13:21:03                  |
+--------------------------------------+

select months_add('2019-01-30 13:21:03', 1);
+--------------------------------------+
| months_add('2019-01-30 13:21:03', 1) |
+--------------------------------------+
| 2019-02-28 13:21:03                  |
+--------------------------------------+
```
