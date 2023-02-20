# years_add

## description

### Syntax

```Haskell
DATETIME YEARS_ADD(DATETIME expr1,INT expr2)
```

Add a specified time interval to the date. Unit is year.

The expr1 parameter is a valid datetime expression.

The expr2 parameter is the years you want to add.

## example

```Plain Text
select years_add('2010-11-30 23:50:50', 2);
+-------------------------------------+
| years_add('2010-11-30 23:50:50', 2) |
+-------------------------------------+
| 2012-11-30 23:50:50                 |
+-------------------------------------+

select years_add('2010-11-30', 2);
+----------------------------+
| years_add('2010-11-30', 2) |
+----------------------------+
| 2012-11-30 00:00:00        |
+----------------------------+
```

## keyword

YEARS_ADD,ADD
