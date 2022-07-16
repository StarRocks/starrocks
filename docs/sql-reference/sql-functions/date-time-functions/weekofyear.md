# weekofyear

## description

## Syntax

```Haskell
INT WEEKOFYEAR(DATETIME date)
```

This function returns the week number for a given date (e.g. within a year).

Parameters must be Date or Datetime type.

## example

```Plain Text
MySQL > select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## keyword

WEEKOFYEAR
