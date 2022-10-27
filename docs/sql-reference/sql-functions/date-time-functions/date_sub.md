# date_sub

## description

### Syntax

```Haskell
INT DATE_SUB(DATETIME date,INTERVAL expr type)
```

<<<<<<< HEAD
Subtract the specified time interval from the date.
=======
## Parameters

- `date`: It must be a valid date expression.
- `expr`: the time interval you want to subtract. It must be of the INT type.
- `type`: It can only be set to any of the following values: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

## Return value
>>>>>>> 50060d4cf ([Doc] add date functions and update other docs (#12589))

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
