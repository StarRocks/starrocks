# hour

## description

### Syntax

```Haskell
INT HOUR(DATETIME date)
```

It returns the hour for a given date. The return values range from 0 to 23.

The parameter is in Date or Datetime type.

## example

```Plain Text
MySQL > select hour('2018-12-31 23:59:59');
+-----------------------------+
| hour('2018-12-31 23:59:59') |
+-----------------------------+
|                          23 |
+-----------------------------+
```

## keyword

HOUR
