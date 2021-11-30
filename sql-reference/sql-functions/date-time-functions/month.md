# month

## description

### Syntax

```Haskell
INT MONTH(DATETIME date)
```

It returns the month for a given date. The return values range from 1 to 12.

The parameter is in Date or Datetime type.

## example

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
