# year

## description

### Syntax

```Haskell
INT YEAR(DATETIME date)
```

This function returns the year part in date type (a number from 1000 to 9999).

The parameter is in Date or Datetime type.

## example

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
