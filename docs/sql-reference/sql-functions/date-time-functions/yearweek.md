# yearweek

## Description

Returns the yearweek number for a given date within a year.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT yearweek(DATETIME date)
```

## Examples

```Plain Text
MySQL > select YEARWEEK('2023-08-09');;
+------------------------+
| yearweek('2023-08-09') |
+------------------------+
|                 202332 |
+------------------------+
```

## keyword

YEARWEEK
