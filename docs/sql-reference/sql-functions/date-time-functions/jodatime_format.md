# jodatime_format

## Description

Converts a date into a string according to the format that is compatible with JodaTime’s DateTimeFormat pattern format. Currently it supports strings with a maximum of 128 bytes. If the length of the returned value exceeds 128, NULL is returned.

## Syntax

```Haskell
VARCHAR JODATIME_FORMAT(DATETIME date, VARCHAR format)
```

## Parameters

- The `date` parameter must be a valid date or date expression.

- `format` specifies the output format of the date or time.

The available formats could refer to [joda-time format](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)

## Examples

```Plain Text
mysql> select jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd');
+------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd') |
+------------------------------------------------------+
| 2020-06-25                                           |
+------------------------------------------------------+

mysql> select jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd HH:mm:ss');
+---------------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'yyyy-MM-dd HH:mm:ss') |
+---------------------------------------------------------------+
| 2020-06-25 15:58:51                                           |
+---------------------------------------------------------------+

mysql> select jodatime_format('2020-06-25 15:58:51', 'MM dd ee EE');
+-------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'MM dd ee EE') |
+-------------------------------------------------------+
| 06 25 04 Thu                                          |
+-------------------------------------------------------+
1 row in set (0.02 sec)

mysql> select jodatime_format('2020-06-25 15:58:51', 'MMM dd ee EEE');
+---------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'MMM dd ee EEE') |
+---------------------------------------------------------+
| Jun 25 04 Thu                                           |
+---------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select jodatime_format('2020-06-25 15:58:51', 'MMMM dd ee EEEE');
+-----------------------------------------------------------+
| jodatime_format('2020-06-25 15:58:51', 'MMMM dd ee EEEE') |
+-----------------------------------------------------------+
| June 25 04 Thursday                                       |
+-----------------------------------------------------------+

mysql> select jodatime_format('2023-06-25 12:00:00', 'KK:mm:ss a');
+------------------------------------------------------+
| jodatime_format('2023-06-25 12:00:00', 'KK:mm:ss a') |
+------------------------------------------------------+
| 00:00:00 PM                                          |
+------------------------------------------------------+

mysql> select jodatime_format('2023-06-25 12:00:00', 'hh:mm:ss a');
+------------------------------------------------------+
| jodatime_format('2023-06-25 12:00:00', 'hh:mm:ss a') |
+------------------------------------------------------+
| 12:00:00 PM                                          |
+------------------------------------------------------+

mysql> select jodatime_format('2023-06-25 00:00:00', 'yyyyMMdd ''starrocks''');
+------------------------------------------------------------------+
| jodatime_format('2023-06-25 00:00:00', 'yyyyMMdd \'starrocks\'') |
+------------------------------------------------------------------+
| 20230625 starrocks                                               |
+------------------------------------------------------------------+
```

## keyword

JODATIME_FORMAT,JODA,FORMAT
