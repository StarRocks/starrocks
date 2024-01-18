---
displayed_sidebar: "English"
---

# str2date

## Description

Converts a string into a DATE value according to the specified format. If the conversion fails, NULL is returned.

The format must be consistent with that described in [date_format](./date_format.md).

This function is equivalent to [str_to_date](../date-time-functions/str_to_date.md) but has a different return type.

## Syntax

```Haskell
DATE str2date(VARCHAR str, VARCHAR format);
```

## Parameters

`str`: the time expression you want to convert. It must be of the VARCHAR type.

`format`: the format used to return the value. For the supported format, see [date_format](./date_format.md).

## Return value

Returns a value of the DATE type.

NULL is returned if `str` or `format` is NULL.

## Examples

```Plain
select str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s');
+------------------------------------------------------+
| str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s') |
+------------------------------------------------------+
| 2010-11-30                                           |
+------------------------------------------------------+
1 row in set (0.01 sec)
```
