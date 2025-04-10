---
displayed_sidebar: docs
---

# str_to_date

## Description

Converts a string into a DATETIME value according to the specified format. If the conversion fails, NULL is returned.

The format must be consistent with that described in [date_format](./date_format.md).

This function is inverse to [date_format](./date_format.md).

## Syntax

```Haskell
DATETIME STR_TO_DATE(VARCHAR str, VARCHAR format)
```

## Parameters

`str`: the time expression you want to convert. It must be of the VARCHAR type.

`format`: the format used to return the value. For the supported format, see [date_format](./date_format.md).

## Return value

Returns a value of the DATETIME type. If `format` specifies a date, a DATE value is returned.

NULL is returned if `str` or `format` is NULL.

## Examples

Example 1: Convert the input into a DATETIME value.

```Plain Text
MySQL > select str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');
+---------------------------------------------------------+
| str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s') |
+---------------------------------------------------------+
| 2014-12-21 12:34:56                                     |
+---------------------------------------------------------+
```

Example 2: Convert the input into a DATE value.

```Plain Text
MySQL > select str_to_date('2014-12-21 12:34:56', '%Y-%m-%d');
+--------------------------------------------------------------+
| str_to_date('2014-12-21 12:34:56', '%Y-%m-%d')               |
+--------------------------------------------------------------+
| 2014-12-21                                                   |
+--------------------------------------------------------------+
```

Example 3: Convert the input "200442 Monday" into a DATE value.

```Plain Text
MySQL > select str_to_date('200442 Monday', '%X%V %W');
+-----------------------------------------+
| str_to_date('200442 Monday', '%X%V %W') |
+-----------------------------------------+
| 2004-10-18                              |
+-----------------------------------------+
```

## keyword

STR_TO_DATE,STR,TO,DATE
