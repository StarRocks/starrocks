# dayofmonth

## description

### Syntax

```Haskell
INT DAYOFMONTH(DATETIME date)
```

Obtain the day information in the date and return values range from 1 to 31.

The parameter is Date or Datetime typ

## example

```Plain Text
MySQL > select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```

## keyword

DAYOFMONTH
