# dayofweek

## description

### Syntax

```Haskell
INT dayofweek(DATETIME date)
```

DAYOFWEEK function returns the weekday index for a given date. (e.g. Monday: 1, Tuesday: 2, Saturday: 7)

The parameter is Date or Datetime type. It can also be casted as numbers in Date or Datetime type.

## example

```Plain Text
MySQL > select dayofweek('2019-06-25');
+----------------------------------+
| dayofweek('2019-06-25 00:00:00') |
+----------------------------------+
|                                3 |
+----------------------------------+

MySQL > select dayofweek(cast(20190625 as date));
+-----------------------------------+
| dayofweek(CAST(20190625 AS DATE)) |
+-----------------------------------+
|                                 3 |
+-----------------------------------+
```

## keyword

DAYOFWEEK
