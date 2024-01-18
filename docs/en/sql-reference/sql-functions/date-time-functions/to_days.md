---
displayed_sidebar: "English"
---

# to_days

## Description

Returns the number of days between a date and 0000-01-01.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT TO_DAYS(DATETIME date)
```

## Examples

```Plain Text
MySQL > select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```

## keyword

TO_DAYS,TO,DAYS
