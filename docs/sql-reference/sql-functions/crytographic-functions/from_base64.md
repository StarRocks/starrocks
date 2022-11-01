# from_base64

## Description

Decodes a Base64-encoded string. This function is an inverse of [to_base64](to_base64.md).

## Syntax

```Haskell
from_base64(str);
```

## Parameters

`str`: the string to decode. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL or an invalid Base64 string, NULL is returned. If the input is empty, an error is returned.

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text
mysql> select from_base64("starrocks");
+--------------------------+
| from_base64('starrocks') |
+--------------------------+
| ²֫®$                       |
+--------------------------+
1 row in set (0.00 sec)

mysql> select from_base64('c3RhcnJvY2tz');
+-----------------------------+
| from_base64('c3RhcnJvY2tz') |
+-----------------------------+
| starrocks                   |
+-----------------------------+
```
