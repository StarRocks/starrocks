# minute

## description

### Syntax

```Haskell
INT MINUTE(DATETIME date)
```

It returns the minute for a given date. The return values range from 0 to 59.

The parameter is in Date or Datetime type.

## example

```Plain Text
MySQL > select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

MINUTE
