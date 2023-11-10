# month

## Description

Returns the month for a given date. The return value ranges from 1 to 12.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT MONTH(DATETIME date)
```

## Examples

```Plain Text
MySQL > select month('1987-01-01');
+-----------------------------+
|month('1987-01-01 00:00:00') |
+-----------------------------+
|                           1 |
+-----------------------------+
```

## keyword

MONTH
