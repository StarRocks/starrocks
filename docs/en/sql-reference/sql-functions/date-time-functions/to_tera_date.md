---
displayed_sidebar: docs
---

# to_tera_date

## Description

Parses a date or time string according to the specified format and converts the string to a DATE value.

## Syntax

```Haskell
DATE to_tera_date(VARCHAR str, VARCHAR format)
```

## Parameters

- `str`: the time expression to convert. It must be of the VARCHAR type.

- `format`: the date format specifier for `str`. It is used to parse and convert the input string. `format` must match `string`. Otherwise, NULL is returned. If `format` is invalid, an error is returned.

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
select to_tera_date("1988/04/08","yyyy/mm/dd");
+------------------------------------------+
| to_tera_date('1988/04/08', 'yyyy/mm/dd') |
+------------------------------------------+
| 1988-04-08                               |
+------------------------------------------+

select to_tera_date("04-08-1988","mm-dd-yyyy");
+------------------------------------------+
| to_tera_date('04-08-1988', 'mm-dd-yyyy') |
+------------------------------------------+
| 1988-04-08                               |
+------------------------------------------+

select to_tera_date(";198804:08",";yyyymm:dd");
+------------------------------------------+
| to_tera_date(';198804:08', ';yyyymm:dd') |
+------------------------------------------+
| 1988-04-08                               |
+------------------------------------------+

select to_tera_date("2020-02-02 00:00:00", "yyyy-mm-dd");
+---------------------------------------------------+
| to_tera_date('2020-02-02 00:00:00', 'yyyy-mm-dd') |
+---------------------------------------------------+
| 2020-02-02                                        |
+---------------------------------------------------+

-- The input is year and does not contain the month or date part. The first day in that year is returned.
select to_tera_date("1988","yyyy");
+------------------------------+
| to_tera_date('1988', 'yyyy') |
+------------------------------+
| 1988-01-01                   |
+------------------------------+
```

## Keywords

TO_TERA_DATE
