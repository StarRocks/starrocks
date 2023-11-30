---
displayed_sidebar: "English"
---

# bitmap_from_binary

## Description

Converts a binary string with a specific format to a bitmap.

This function can be used to load bitmap data to StarRocks.

This function is supported from v3.0.

## Syntax

```Haskell
BITMAP bitmap_from_binary(VARBINARY str)
```

## Parameters

`str`: The supported data type is VARBINARY.

## Return value

Returns a value of the BITMAP type.

## Examples

Example 1: Use this function with other Bitmap functions.

```Plain
mysql> select bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string("0,1,2,3"))));
+---------------------------------------------------------------------------------------+
| bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string('0,1,2,3')))) |
+---------------------------------------------------------------------------------------+
| 0,1,2,3                                                                               |
+---------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
