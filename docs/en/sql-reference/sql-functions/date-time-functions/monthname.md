---
displayed_sidebar: docs
---

# monthname

## Description

Returns the name of the month for a given date.  

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
VARCHAR MONTHNAME(date)
```

## Examples

```Plain Text
MySQL > select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
```

## keyword

MONTHNAME, monthname
