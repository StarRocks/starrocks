---
displayed_sidebar: docs
---

# timediff

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the difference between two DATETIME expressions.

The return value must be of the TIME type.

## Syntax

```Haskell
TIME TIMEDIFF(DATETIME expr1, DATETIME expr2)`
```

## Examples

```Plain Text
MySQL > SELECT TIMEDIFF(now(),utc_timestamp());
+----------------------------------+
| timediff(now(), utc_timestamp()) |
+----------------------------------+
| 08:00:00                         |
+----------------------------------+

MySQL > SELECT TIMEDIFF('2019-07-11 16:59:30','2019-07-11 16:59:21');
+--------------------------------------------------------+
| timediff('2019-07-11 16:59:30', '2019-07-11 16:59:21') |
+--------------------------------------------------------+
| 00:00:09                                               |
+--------------------------------------------------------+

MySQL > SELECT TIMEDIFF('2019-01-01 00:00:00', NULL);
+---------------------------------------+
| timediff('2019-01-01 00:00:00', NULL) |
+---------------------------------------+
| NULL                                  |
+---------------------------------------+
```

## keyword

TIMEDIFF
