---
displayed_sidebar: "English"
---

# dayofweek_iso

## Description

Returns the ISO standard weekday number for a given date.

The date parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT DAY_OF_WEEK_ISO(DATETIME date)
```

## Examples

```Plain Text
mysql> select dayofweek_iso("2023-01-01");
+-----------------------------+
| dayofweek_iso('2023-01-01') |
+-----------------------------+
|                           7 |
+-----------------------------+
```

## keyword

DAY_OF_WEEK_ISO