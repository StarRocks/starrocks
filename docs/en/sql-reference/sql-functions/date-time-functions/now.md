---
displayed_sidebar: "English"
---

# now, current_timestamp, localtime, localtimestamp

## Description

Returns the current date and time. Since v3.1, the result is accurate to the microsecond.

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/timezone.md).

## Syntax

```Haskell
DATETIME NOW()
```

## Examples

```Plain Text
MySQL > select now();
+---------------------+
| now()               |
+---------------------+
| 2019-05-27 15:58:25 |
+---------------------+

-- The result is accurate to the microsecond since v3.1.
MySQL > select now();
+----------------------------+
| now()                      |
+----------------------------+
| 2023-11-18 12:54:34.878000 |
+----------------------------+
```

## keyword

NOW, now
