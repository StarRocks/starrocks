---
displayed_sidebar: docs
---

# curtime,current_time

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Obtains the current time and returns a value of the TIME type.

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
TIME CURTIME()
```

## Examples

```Plain Text
MySQL > select current_time();
+----------------+
| current_time() |
+----------------+
| 15:25:47       |
+----------------+
```

## keyword

CURTIME,CURRENT_TIME
