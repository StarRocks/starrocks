---
displayed_sidebar: docs
---

# dayofmonth

## Description

Obtains the day part in a date and returns a value that ranges from 1 to 31.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT DAYOFMONTH(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```

## keyword

DAYOFMONTH
