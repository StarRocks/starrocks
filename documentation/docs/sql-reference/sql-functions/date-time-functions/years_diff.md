---
displayed_sidebar: "English"
---

# years_diff

## Description

Returns the year difference between two date expressions (`expr1` − `expr2`), accurate to the year.

## Syntax

```Haskell
BIGINT years_diff(DATETIME expr1,DATETIME expr2);
```

## Parameters

- `expr1`: the end time. It must be of the DATETIME or DATE type.

- `expr2`: the start time. It must be of the DATETIME or DATE type.

## Return value

Returns a BIGINT value.

NULL is returned if the date does not exist, for example, 2022-02-29.

## Examples

```Plain
select years_diff('2010-11-30 23:59:59', '2000-11-1 23:59:59');
+---------------------------------------------------------+
| years_diff('2010-11-30 23:59:59', '2000-11-1 23:59:59') |
+---------------------------------------------------------+
|                                                      10 |
+---------------------------------------------------------+

select years_diff('2010-11-30', '2000-11-1');
+---------------------------------------+
| years_diff('2010-11-30', '2000-11-1') |
+---------------------------------------+
|                                    10 |
+---------------------------------------+
```
