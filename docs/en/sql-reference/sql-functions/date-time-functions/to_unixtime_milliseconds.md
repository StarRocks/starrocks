---
displayed_sidebar: "English"
---

# to_unixtime_milliseconds

Converts a DATE or DATETIME value into a UNIX timestamp in milliseconds.

The `date` parameter must be of the DATE or DATETIME type.

For time before 1970-01-01 00:00:00 or after 2038-01-19 11:14:07, this function returns 0.

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
INT to_unixtime_milliseconds(DATETIME date)
```

## Examples

```Plain Text
select to_unixtime_milliseconds('2000-12-01 12:30:01.1002');
+------------------------------------------------------+
| to_unixtime_milliseconds('2000-12-01 12:30:01.1002') |
+------------------------------------------------------+
|                                         975673801100 |
+------------------------------------------------------+

select to_unixtime_milliseconds('2000-12-01 12:30:01.100');
+-----------------------------------------------------+
| to_unixtime_milliseconds('2000-12-01 12:30:01.100') |
+-----------------------------------------------------+
|                                        975673801100 |
+-----------------------------------------------------+

select to_unixtime_milliseconds('2000-12-01 12:30:01.100000');
+--------------------------------------------------------+
| to_unixtime_milliseconds('2000-12-01 12:30:01.100000') |
+--------------------------------------------------------+
|                                           975673801100 |
+--------------------------------------------------------+
```
