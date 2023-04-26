# year

## Description

Returns the year part in a date and returns a value that ranges from 1000 to 9999.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT YEAR(DATETIME date)
```

## Examples

```Plain Text
MySQL > select year('1987-01-01');
+-----------------------------+
| year('1987-01-01 00:00:00') |
+-----------------------------+
|                        1987 |
+-----------------------------+
```

## keyword

YEAR
