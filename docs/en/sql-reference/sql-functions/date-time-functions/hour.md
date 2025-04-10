---
displayed_sidebar: docs
---

# hour

## Description

Returns the hour for a given date. The return value ranges from 0 to 23.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT HOUR(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select hour('2018-12-31 23:59:59');
+-----------------------------+
| hour('2018-12-31 23:59:59') |
+-----------------------------+
|                          23 |
+-----------------------------+
```

## keyword

HOUR
