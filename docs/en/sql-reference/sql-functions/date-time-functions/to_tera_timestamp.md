---
displayed_sidebar: "English"
---

# to_tera_timestamp

## Description

 Converts a VARCHAR value into a DATETIME from an input format.

## Syntax

```Haskell

DATETIME to_tera_timestamp(VARCHAR str, VARCHAR format)
```

## Parameters
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
+-----------------------------------------------------------+
| to_tera_timestamp('1988/04/08 2:3:4', 'yyyy/mm/dd hh24:mi:ss') |
+-----------------------------------------------------------+
| 1988-04-08 02:03:04                                       |
+-----------------------------------------------------------+
```

## keyword

to_tera_timestamp
