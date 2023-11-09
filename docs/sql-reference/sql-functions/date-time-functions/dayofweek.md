---
displayed_sidebar: "English"
---

# dayofweek

## Description

Returns the weekday index for a given date. For example, the index for Sunday is 1, for Monday is 2, for Saturday is 7.

The `date` parameter must be of the DATE or DATETIME type, or a valid expression that can be cast into a DATE or DATETIME value.

## Syntax

```Haskell
INT dayofweek(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofweek('2019-06-25');
+----------------------------------+
| dayofweek('2019-06-25 00:00:00') |
+----------------------------------+
|                                3 |
+----------------------------------+

MySQL > select dayofweek(cast(20190625 as date));
+-----------------------------------+
| dayofweek(CAST(20190625 AS DATE)) |
+-----------------------------------+
|                                 3 |
+-----------------------------------+
```

## keyword

DAYOFWEEK
