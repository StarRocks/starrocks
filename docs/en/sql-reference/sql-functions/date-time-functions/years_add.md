---
displayed_sidebar: "English"
---

# years_add

## Description

Adds a specified time interval to the date. Unit is year.

### Syntax

```Haskell
DATETIME YEARS_ADD(DATETIME|DATE expr1,INT expr2)
```

The expr1 parameter is a valid datetime or date expression.

The expr2 parameter is the years you want to add.

## Examples

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
