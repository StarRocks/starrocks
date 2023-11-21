---
displayed_sidebar: "English"
---

# day

## Description

Extracts the day part of a date or datetime expression and returns a value that ranges from 1 to 31.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT DAY(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select day('1987-01-31');
+----------------------------+
| day('1987-01-31 00:00:00') |
+----------------------------+
|                         31 |
+----------------------------+

MySQL > select day('1987-01-31 20:10:59');
+----------------------------+
| day('1987-01-31 20:10:59') |
+----------------------------+
|                         31 |
+----------------------------+
```

## keywords

DAY, day
