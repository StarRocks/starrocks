---
displayed_sidebar: "English"
---

# seconds_add

## Description

Adds a specified time interval to the datetime. Unit is second.

### Syntax

```Haskell
DATETIME SECONDS_ADD(DATETIME expr1,INT expr2)
```

The `expr1` parameter is a valid datetime expression.

The `expr2` parameter is the seconds you want to add.

## Examples

```Plain Text
select seconds_add('2010-11-30 23:50:50', 2);
+---------------------------------------+
| seconds_add('2010-11-30 23:50:50', 2) |
+---------------------------------------+
| 2010-11-30 23:50:52                   |
+---------------------------------------+

select seconds_add('2010-11-30', 2);
+------------------------------+
| seconds_add('2010-11-30', 2) |
+------------------------------+
| 2010-11-30 00:00:02          |
+------------------------------+
```

## keyword

SECONDS_ADD,ADD
