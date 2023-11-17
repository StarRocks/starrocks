---
displayed_sidebar: "English"
---

# hex

## Description

If `x` is a numerical value, this function returns a hexadecimal string representation of the value.

If `x` is a string, this function returns a hexadecimal string representation of the string where each character in the string is converted into two hexadecimal digits.

## Syntax

```Haskell
HEX(x);
```

## Parameters

`x`: the string or number to convert. The supported data types are BIGINT, VARCHAR, and VARBINARY (v3.0 and later).

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select hex(3);
+--------+
| hex(3) |
+--------+
| 3      |
+--------+
1 row in set (0.00 sec)

mysql> select hex('3');
+----------+
| hex('3') |
+----------+
| 33       |
+----------+
1 row in set (0.00 sec)

mysql> select hex('apple');
+--------------+
| hex('apple') |
+--------------+
| 6170706C65   |
+--------------+

-- The input is a binary value.

mysql> select hex(x'abab');
+-------------+
| hex('ABAB') |
+-------------+
| ABAB        |
+-------------+
1 row in set (0.01 sec)
```

## Keywords

HEX
