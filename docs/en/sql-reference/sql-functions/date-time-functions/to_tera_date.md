---
displayed_sidebar: "English"
---

# to_date

## Description

 Converts a VARCHAR value into a date from an input format.

## Syntax

```Haskell

Converts a VARCHAR value into a date from an input format.

DATE to_tera_date(VARCHAR str, VARCHAR format)
```

## Parameters

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

TO_TERA_DATE
