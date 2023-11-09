---
displayed_sidebar: "English"
---

# unix_timestamp

## Description

Converts a DATE or DATETIME value into a UNIX timestamp.

If no parameter is specified, this function converts the current time into a UNIX timestamp.

The `date` parameter must be of the DATE or DATETIME type.

For time before 1970-01-01 00:00:00 or after 2038-01-19 11:14:07, this function returns 0.

For more information about the date format, see [date_format](./date_format.md).

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
BIGINT UNIX_TIMESTAMP()
BIGINT UNIX_TIMESTAMP(DATETIME date)
BIGINT UNIX_TIMESTAMP(DATETIME date, STRING fmt)
```

## Examples

```Plain Text
MySQL > select unix_timestamp();
+------------------+
| unix_timestamp() |
+------------------+
|       1558589570 |
+------------------+

MySQL > select unix_timestamp('2007-11-30 10:30:19');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30:19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('2007-11-30 10:30-19', '%Y-%m-%d %H:%i-%s');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30-19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s');
+---------------------------------------+
|unix_timestamp('2007-11-30 10:30%3A19')|
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('1969-01-01 00:00:00');
+---------------------------------------+
| unix_timestamp('1969-01-01 00:00:00') |
+---------------------------------------+
|                                     0 |
+---------------------------------------+
```

## keyword

UNIX_TIMESTAMP,UNIX,TIMESTAMP
