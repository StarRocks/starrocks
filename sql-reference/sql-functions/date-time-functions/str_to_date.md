# str_to_date

## description

### Syntax

```Haskell
DATETIME STR_TO_DATE(VARCHAR str, VARCHAR format)
```

Convert str to DATE type according to the specified format. If the converted result is wrong, NULL will be returned.

Supported format should be consistent with date_format.

## example

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
