---
displayed_sidebar: docs
---

# weeks_diff

## Description

Returns the week difference between two date expressions (*`expr1`* − *`expr2`*), accurate to the week.

## Syntax

```Haskell
BIGINT weeks_diff(DATETIME expr1,DATETIME expr2);
```

## Parameters

- `expr1`: the end time. It must be of the DATETIME or DATE type.

- `expr2`: the start time. It must be of the DATETIME or DATE type.

## Return value

Returns a BIGINT value.

NULL is returned if the date does not exist, for example, 2022-02-29.

## Examples

```Plain
select weeks_diff('2010-11-30 23:59:59', '2010-1-1 23:59:59');
+--------------------------------------------------------+
| weeks_diff('2010-11-30 23:59:59', '2010-1-1 23:59:59') |
+--------------------------------------------------------+
|                                                     47 |
+--------------------------------------------------------+

select weeks_diff(current_time(), '2010-11-30 23:59:59');
+---------------------------------------------------+
| weeks_diff(current_time(), '2010-11-30 23:59:59') |
+---------------------------------------------------+
|                                               619 |
+---------------------------------------------------+

select weeks_diff('2010-11-30 23:59:59', '2010-11-24 23:59:59');
+----------------------------------------------------------+
| weeks_diff('2010-11-30 23:59:59', '2010-11-24 23:59:59') |
+----------------------------------------------------------+
|                                                        0 |
+----------------------------------------------------------+
```
