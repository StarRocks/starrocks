---
displayed_sidebar: "English"
---

# to_base64

## Description

Converts a string into a Base64-encoded string. This function is an inverse of [from_base64](from_base64.md).

## Syntax

```Haskell
to_base64(str);
```

## Parameters

`str`: the string to encode. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL, NULL is returned. If the input is empty, an error is returned.

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text
mysql> select to_base64("starrocks");
+------------------------+
| to_base64('starrocks') |
+------------------------+
| c3RhcnJvY2tz           |
+------------------------+
1 row in set (0.00 sec)

mysql> select to_base64(123);
+----------------+
| to_base64(123) |
+----------------+
| MTIz           |
+----------------+
```
