---
displayed_sidebar: docs
---

# months_diff

## Description

Returns the month difference between two date expressions (`expr1` − `expr2`), accurate to the month.

## Syntax

```Haskell
BIGINT months_diff(DATETIME expr1,DATETIME expr2);
```

## Parameters

- `expr1`: the end time. It must be of the DATETIME or DATE type.

- `expr2`: the start time. It must be of the DATETIME or DATE type.

## Return value

Returns a BIGINT value.

NULL is returned if the date does not exist, for example, 2022-02-29.

## Examples

```Plain
select months_diff('2010-11-30 23:59:59', '2010-1-1 23:59:59');
+---------------------------------------------------------+
| months_diff('2010-11-30 23:59:59', '2010-1-1 23:59:59') |
+---------------------------------------------------------+
|                                                      10 |
+---------------------------------------------------------+

select months_diff('2010-11-30', '2010-1-1');
+---------------------------------------+
| months_diff('2010-11-30', '2010-1-1') |
+---------------------------------------+
|                                    10 |
+---------------------------------------+
```
