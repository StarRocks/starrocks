---
displayed_sidebar: "English"
---

# to_tera_timestamp

## Description

Converts the specified VARCHAR value into a DATETIME value in the specified format.

## Syntax

```Haskell
DATETIME to_tera_timestamp(VARCHAR str, VARCHAR format)
```

## Parameters

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
+-----------------------------------------------------------+
| to_tera_timestamp('1988/04/08 2:3:4', 'yyyy/mm/dd hh24:mi:ss') |
+-----------------------------------------------------------+
| 1988-04-08 02:03:04                                       |
+-----------------------------------------------------------+
```

## Keywords

TO_TERA_TIMESTAMP
