---
displayed_sidebar: docs
---

# weekday

Returns the weekday index for a given date. For example, the index for Monday is 0, for Sunday is 6.

## Syntax

```Haskell
INT WEEKDAY(DATETIME date)
```

## Parameters

`date`: The `date` parameter must be of the DATE or DATETIME type, or a valid expression that can be cast into a DATE or DATETIME value.

## Examples

```Plain Text
MySQL > select weekday('2023-01-01');
+-----------------------+
| weekday('2023-01-01') |
+-----------------------+
|                     6 |
+-----------------------+
```

## keyword

WEEKDAY