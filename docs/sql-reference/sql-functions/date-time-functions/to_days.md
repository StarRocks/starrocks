# to_days

## description

### Syntax

```Haskell
INT TO_DAYS(DATETIME date)
```

to_days returns the number of days between a date and 0000-01-01.

The parameter should be Date or Datetime type.

## example

```Plain Text
MySQL > select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```

## keyword

TO_DAYS,TO,DAYS
