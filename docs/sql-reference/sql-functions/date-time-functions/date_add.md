# date_add

## description

### Syntax

```Haskell
INT DATE_ADD(DATETIME date,INTERVAL expr type)
```

Add a specified time interval to the date.

The date parameter is a valid data expression. .

The expr parameter is the time interval you want to add.

The type parameter could be the following values: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

## example

```Plain Text
MySQL > select date_add('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_add('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-12-02 23:59:59                             |
+-------------------------------------------------+
```

## keyword

DATE_ADD,DATE,ADD
