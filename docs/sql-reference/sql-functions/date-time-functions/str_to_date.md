# str_to_date

## Description

Converts a string to a DATE value according to the specified format. If the conversion fails, NULL is returned.

The format must be consistent with that described in [date_format](./date_format.md).

## Syntax

```Haskell
DATETIME STR_TO_DATE(VARCHAR str, VARCHAR format)
```

## Examples

```Plain Text
MySQL > select str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');
+---------------------------------------------------------+
| str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s') |
+---------------------------------------------------------+
| 2014-12-21 12:34:56                                     |
+---------------------------------------------------------+

MySQL > select str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');
+--------------------------------------------------------------+
| str_to_date('2014-12-21 12:34%3A56', '%Y-%m-%d %H:%i:%s') |
+--------------------------------------------------------------+
| 2014-12-21 12:34:56                                          |
+--------------------------------------------------------------+

MySQL > select str_to_date('200442 Monday', '%X%V %W');
+-----------------------------------------+
| str_to_date('200442 Monday', '%X%V %W') |
+-----------------------------------------+
| 2004-10-18                              |
+-----------------------------------------+
```

## keyword

STR_TO_DATE,STR,TO,DATE
