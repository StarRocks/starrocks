---
displayed_sidebar: "English"
---

# now, current_timestamp, localtime, localtimestamp

## Description

Returns the current date and time.

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
```

## keyword

NOW, now
