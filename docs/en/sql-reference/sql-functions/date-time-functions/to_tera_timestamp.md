---
displayed_sidebar: docs
---

# to_tera_timestamp

## Description

Parses a date or time string according to the specified format and converts the string to a DATETIME  value.

## Syntax

```Haskell
DATETIME to_tera_timestamp(VARCHAR str, VARCHAR format)
```

## Parameters

- `str`: the time expression to convert. It must be of the VARCHAR type.

- `format`: the time format specifier for `str`. It is used to parse and convert the input string. `format` must match `string`. Otherwise, NULL is returned. If `format` is invalid, an error is returned.

  The following table describes the format elements.

  | **Element**           | **Description**                             |
  | --------------------- | ------------------------------------------- |
  | [ \r \n \t - / , . ;] | Punctuation characters that are ignored in conversion       |
  | dd                    | Day of month (1 - 31)                       |
  | hh                    | Hour of day (1 - 12)                        |
  | hh24                  | Hour of day (0 - 23)                        |
  | mi                    | Minute (0 - 59)                             |
  | mm                    | Month (01 - 12)                             |
  | ss                    | Second (0 - 59)                             |
  | yyyy                  | 4-digit year.                               |
  | yy                    | 2-digit year.                               |
  | am                    | Meridian indicator.                         |
  | pm                    | Meridian indicator.                         |

## Examples

```SQL
select to_tera_timestamp("1988/04/08","yyyy/mm/dd");
+-----------------------------------------------+
| to_tera_timestamp('1988/04/08', 'yyyy/mm/dd') |
+-----------------------------------------------+
| 1988-04-08 00:00:00                           |
+-----------------------------------------------+

select to_tera_timestamp("04-08-1988","mm-dd-yyyy");
+-----------------------------------------------+
| to_tera_timestamp('04-08-1988', 'mm-dd-yyyy') |
+-----------------------------------------------+
| 1988-04-08 00:00:00                           |
+-----------------------------------------------+

select to_tera_timestamp("04.1988,08","mm.yyyy,dd");
+-----------------------------------------------+
| to_tera_timestamp('04.1988,08', 'mm.yyyy,dd') |
+-----------------------------------------------+
| 1988-04-08 00:00:00                           |
+-----------------------------------------------+

select to_tera_timestamp("1988/04/08 2","yyyy/mm/dd hh");
+----------------------------------------------------+
| to_tera_timestamp('1988/04/08 2', 'yyyy/mm/dd hh') |
+----------------------------------------------------+
| 1988-04-08 02:00:00                                |
+----------------------------------------------------+

select to_tera_timestamp("1988/04/08 14","yyyy/mm/dd hh24");
+-------------------------------------------------------+
| to_tera_timestamp('1988/04/08 14', 'yyyy/mm/dd hh24') |
+-------------------------------------------------------+
| 1988-04-08 14:00:00                                   |
+-------------------------------------------------------+

select to_tera_timestamp("1988/04/08 14:15","yyyy/mm/dd hh24:mi");
+-------------------------------------------------------------+
| to_tera_timestamp('1988/04/08 14:15', 'yyyy/mm/dd hh24:mi') |
+-------------------------------------------------------------+
| 1988-04-08 14:15:00                                         |
+-------------------------------------------------------------+

select to_tera_timestamp("1988/04/08 2:3:4","yyyy/mm/dd hh24:mi:ss");
+----------------------------------------------------------------+
| to_tera_timestamp('1988/04/08 2:3:4', 'yyyy/mm/dd hh24:mi:ss') |
+----------------------------------------------------------------+
| 1988-04-08 02:03:04                                            |
+----------------------------------------------------------------+

select to_tera_timestamp("1988/04/08 02 am:3:4","yyyy/mm/dd hh am:mi:ss");
+---------------------------------------------------------------------+
| to_tera_timestamp('1988/04/08 02 am:3:4', 'yyyy/mm/dd hh am:mi:ss') |
+---------------------------------------------------------------------+
| 1988-04-08 02:03:04                                                 |
+---------------------------------------------------------------------+
```

## Keywords

TO_TERA_TIMESTAMP
