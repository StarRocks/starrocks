# day

## Description

Obtains the day part in a date and returns a value that ranges from 1 to 31.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT DAY(DATETIME date)
```

## Examples

```Plain Text
MySQL > select day('1987-01-31');
+----------------------------+
| day('1987-01-31 00:00:00') |
+----------------------------+
|                         31 |
+----------------------------+
```

## keywords

DAY, day
