---
displayed_sidebar: "English"
---

# unhex

## Description

This function performs the opposite operation of hex().

It interprets each pair of hexadecimal digits in the input string as a number and converts it to the byte represented by the number. The return value is a binary string.

## Syntax

```Haskell
UNHEX(str);
```

## Parameters

`str`: the string to convert. The supported data type is VARCHAR. An empty string is returned if any of the following situations occur:

- The length of the string is 0 or the number of characters in the string is an odd number.
- The string contains characters other than `[0-9]`, `[a-z]`, and `[A-Z]`.

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select unhex('33');
+-------------+
| unhex('33') |
+-------------+
| 3           |
+-------------+

mysql> select unhex('6170706C65');
+---------------------+
| unhex('6170706C65') |
+---------------------+
| apple               |
+---------------------+

mysql> select unhex('4142@');
+----------------+
| unhex('4142@') |
+----------------+
|                |
+----------------+
1 row in set (0.01 sec)
```

## Keywords

UNHEX
