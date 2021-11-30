# monthname

## description

### Syntax

```Haskell
VARCHAR MONTHNAME(DATE)
```

It returns the name of the month for a given date.  

The parameter is in Date or Datetime type.

## example

```Plain Text
MySQL > select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
```

## keyword

MONTHNAME
