---
displayed_sidebar: "English"
---

# curtime,current_time

## Description

Obtains the current time and returns a value of the TIME type.

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/timezone.md).

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
