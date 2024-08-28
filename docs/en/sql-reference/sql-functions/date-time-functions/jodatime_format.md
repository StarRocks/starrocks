---
displayed_sidebar: docs
---

# jodatime_format

## Description

Converts the specified date into a string in the specified Joda DateTimeFormat pattern format.

## Syntax

```Haskell
VARCHAR JODATIME_FORMAT(DATETIME | DATE date, VARCHAR format)
```

## Parameters

- `date`: the date you want to convert. It must be a valid date or date expression.
- `format`: the format of the date and time to be returned. For information about the available formats, see [Joda-Time format](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html).

## Return value

Currently, the return value is a string that contains a maximum of 128 bytes in length. If the length of the string converted from the specified date exceeds 128 bytes, `NULL` is returned.

## Examples

Example 1: Convert the date `2020-06-25 15:58:51` into a string in `yyyy-MM-dd` format.

```SQL
MySQL > select jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd');
+------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd') |
+------------------------------------------------------+
| 2020-06-25                                           |
+------------------------------------------------------+
```

Example 2: Convert the date `2020-06-25 15:58:51` into a string in `yyyy-MM-dd HH:mm:ss` format.

```SQL
MySQL > select jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd HH:mm:ss');
+---------------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd HH:mm:ss') |
+---------------------------------------------------------------+
| 2020-06-25 15:58:51                                           |
+---------------------------------------------------------------+
```

Example 3: Convert the date `2020-06-25 15:58:51` into a string in `MM dd ee EE` format.

```SQL
MySQL > select jodatime_format('2020-06-25 15:58:51', 'MM dd ee EE');
+-------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'MM dd ee EE') |
+-------------------------------------------------------+
| 06 25 04 Thu                                          |
+-------------------------------------------------------+
```

Example 4: Convert the date `2020-06-25 15:58:51` into a string in `MMM dd ee EEE` format.

```SQL
MySQL > select jodatime_format('2020-06-25 15:58:51', 'MMM dd ee EEE');
+---------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'MMM dd ee EEE') |
+---------------------------------------------------------+
| Jun 25 04 Thu                                           |
+---------------------------------------------------------+
```

Example 5: Convert the date `2020-06-25 15:58:51` into a string in `MMMM dd ee EEEE` format.

```SQL
MySQL > select jodatime_format('2020-06-25 15:58:51', 'MMMM dd ee EEEE');
+-----------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'MMMM dd ee EEEE') |
+-----------------------------------------------------------+
| June 25 04 Thursday                                       |
+-----------------------------------------------------------+
```

Example 6: Convert the date `2023-06-25 12:00:00` into a string in `KK:mm:ss a` format.

```SQL
MySQL > select jodatime_format('2023-06-25 12:00:00', 'KK:mm:ss a');
+------------------------------------------------------+
| jodatime_format('2023-06-25 12:00:00', 'KK:mm:ss a') |
+------------------------------------------------------+
| 00:00:00 PM                                          |
+------------------------------------------------------+
```

Example 7: Convert the date `2023-06-25 12:00:00` into a string in `hh:mm:ss a` format.

```SQL
MySQL > select jodatime_format('2023-06-25 12:00:00', 'hh:mm:ss a');
+------------------------------------------------------+
| jodatime_format('2023-06-25 12:00:00', 'hh:mm:ss a') |
+------------------------------------------------------+
| 12:00:00 PM                                          |
+------------------------------------------------------+
```

Example 8: Convert the date `2023-06-25 00:00:00` into a string in `yyyyMMdd ''starrocks''` format.

```SQL
MySQL > select jodatime_format('2023-06-25 00:00:00', 'yyyyMMdd ''starrocks''');
+------------------------------------------------------------------+
| jodatime_format('2023-06-25 00:00:00', 'yyyyMMdd \'starrocks\'') |
+------------------------------------------------------------------+
| 20230625 starrocks                                               |
+------------------------------------------------------------------+
```

## Keywords

JODATIME_FORMAT, JODA, FORMAT
