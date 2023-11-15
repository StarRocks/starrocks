# second

## description

### Syntax

```Haskell
INT SECOND(DATETIME date)
```

It returns the seconds for a given date. The return values range from 0 to 59.

The parameter is in Date or Datetime type.

## example

```Plain Text
MySQL > select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

SECOND
