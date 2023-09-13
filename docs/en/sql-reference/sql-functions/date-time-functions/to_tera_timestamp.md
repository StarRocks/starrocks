<<<<<<< HEAD:docs/en/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
---
displayed_sidebar: "English"
---

=======
>>>>>>> c78694acaa ([Feature] Support to_tera_date/to_tera_timestamp (#28509)):docs/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
# to_tera_timestamp

## Description

<<<<<<< HEAD:docs/en/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
Converts the specified VARCHAR value into a DATETIME value in the specified format.
=======
 Converts a VARCHAR value into a DATETIME from an input format.
>>>>>>> c78694acaa ([Feature] Support to_tera_date/to_tera_timestamp (#28509)):docs/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md

## Syntax

```Haskell
<<<<<<< HEAD:docs/en/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
=======

>>>>>>> c78694acaa ([Feature] Support to_tera_date/to_tera_timestamp (#28509)):docs/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
DATETIME to_tera_timestamp(VARCHAR str, VARCHAR format)
```

## Parameters
<<<<<<< HEAD:docs/en/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md

- `str`: the time expression you want to convert. It must be of the VARCHAR type.

- `format`: the format of the DATETIME value to be returned.

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

The following example converts the VARCHAR value `1988/04/08 2:3:4` into a DATETIME value in `yyyy/mm/dd hh24:mi:ss` format.

```SQL
MySQL > select to_tera_timestamp("1988/04/08 2:3:4","yyyy/mm/dd hh24:mi:ss");
=======
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
mysql> select to_tera_timestamp("1988/04/08 2:3:4","yyyy/mm/dd hh24:mi:ss");
>>>>>>> c78694acaa ([Feature] Support to_tera_date/to_tera_timestamp (#28509)):docs/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
+-----------------------------------------------------------+
| to_tera_timestamp('1988/04/08 2:3:4', 'yyyy/mm/dd hh24:mi:ss') |
+-----------------------------------------------------------+
| 1988-04-08 02:03:04                                       |
+-----------------------------------------------------------+
```

<<<<<<< HEAD:docs/en/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
## Keywords

TO_TERA_TIMESTAMP
=======
## keyword

to_tera_timestamp
>>>>>>> c78694acaa ([Feature] Support to_tera_date/to_tera_timestamp (#28509)):docs/sql-reference/sql-functions/date-time-functions/to_tera_timestamp.md
