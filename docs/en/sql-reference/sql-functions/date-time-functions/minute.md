---
displayed_sidebar: docs
---

# minute

## Description

Returns the minute for a given date. The return value ranges from 0 to 59.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT MINUTE(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

MINUTE
