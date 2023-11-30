---
displayed_sidebar: "English"
---

<<<<<<< HEAD
# to_date

## Description

 Converts a VARCHAR value into a date from an input format.
=======
# to_tera_date

## Description

Converts the specified VARCHAR value into a date in the specified format.
>>>>>>> 510a3a3eb3 ([Doc] Add 7 date functions to Branch 3.2 (#36130))

## Syntax

```Haskell
<<<<<<< HEAD

Converts a VARCHAR value into a date from an input format.

=======
>>>>>>> 510a3a3eb3 ([Doc] Add 7 date functions to Branch 3.2 (#36130))
DATE to_tera_date(VARCHAR str, VARCHAR format)
```

## Parameters

<<<<<<< HEAD
Converts a VARCHAR value into a date from an input format.

- `str`: the time expression you want to convert. It must be of the VARCHAR type.
- `format`: the DateTime format as below:

```
[ \r \n \t - / , . ;] :	Punctuation characters are ignored
dd	                  : Day of month (1-31)
hh	                  : Hour of day (1-12)
hh24                  : Hour of the day (0-23)
mi                    : Minute (0-59)
mm                    : Month (01-12)
ss                    : Second (0-59)
yyyy                  : 4-digit year
yy                    : 2-digit year
am                    : Meridian indicator
pm                    : Meridian indicator
```

## Examples

```Plain Text
Converts a VARCHAR value into a date from an input format.

mysql> select to_date("1988/04/08","yyyy/mm/dd");
+-------------------------------------+
| to_date('1988/04/08', 'yyyy/mm/dd') |
+-------------------------------------+
| 1988-04-08                          |
+-------------------------------------+

```

## keyword
=======
- `str`: the time expression you want to convert. It must be of the VARCHAR type.

- `format`: the format of the date and time to be returned.

  The following table describes the format elements.

  | **Element**           | **Description**                             |
  | --------------------- | ------------------------------------------- |
  | [ \r \n \t - / , . ;] | Punctuation characters are ignored.         |
  | dd                    | Day of the month. Valid values: `1` - `31`. |
  | hh                    | Hour of the day. Valid values: `1` - `12`.  |
  | hh24                  | Hour of the day. Valid values: `0` - `23`.  |
  | mi                    | Minute. Valid values: `0` - `59`.           |
  | mm                    | Month. Valid values: `01` - `12`.           |
  | ss                    | Second. Valid values: `0` - `59`.           |
  | yyyy                  | 4-digit year.                               |
  | yy                    | 2-digit year.                               |
  | am                    | Meridian indicator.                         |
  | pm                    | Meridian indicator.                         |

## Examples

The following example converts the VARCHAR value `1988/04/08` into a date in `yyyy/mm/dd` format:

```SQL
MySQL > select to_tera_date("1988/04/08","yyyy/mm/dd");
+------------------------------------------+
| to_tera_date('1988/04/08', 'yyyy/mm/dd') |
+------------------------------------------+
| 1988-04-08                               |
+------------------------------------------+
```

## Keywords
>>>>>>> 510a3a3eb3 ([Doc] Add 7 date functions to Branch 3.2 (#36130))

TO_TERA_DATE
