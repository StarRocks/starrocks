# date_sub

## description

### Syntax

```Haskell
INT DATE_SUB(DATETIME date,INTERVAL expr type)
```

Subtract the specified time interval from the date.

- Date parameter is a valid date expression.
- Expr parameter is the time interval you want to add.
- Type parameters could be the following values: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND

## example

```Plain Text
MySQL > select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+
```

## keyword

DATE_SUB,DATE,SUB
