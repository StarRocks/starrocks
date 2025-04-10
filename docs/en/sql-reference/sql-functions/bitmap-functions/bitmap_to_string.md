---
displayed_sidebar: docs
---

# bitmap_to_string

## Description

Converts an input bitmap into a string that is separated by commas (,). This string contains all the bits in the bitmap. If the input is null, null is returned.

## Syntax

```Haskell
VARCHAR BITMAP_TO_STRING(BITMAP input)
```

## Parameters

`input`: the bitmap you want to convert.

## Return value

Returns a value of the VARCHAR type.

## Examples

Example 1: The input is null and null is returned.

```Plain Text
MySQL > select bitmap_to_string(null);
+------------------------+
| bitmap_to_string(NULL) |
+------------------------+
| NULL                   |
+------------------------+
```

Example 2: The input is empty and an empty string is returned.

```Plain Text
MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+
```

Example 3: Convert a bitmap that contains one bit into a string.

```Plain Text
MySQL > select bitmap_to_string(to_bitmap(1));
+--------------------------------+
| bitmap_to_string(to_bitmap(1)) |
+--------------------------------+
| 1                              |
+--------------------------------+
```

Example 4: Convert a bitmap that contains two bits into a string.

```Plain Text
MySQL > select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));
+---------------------------------------------------------+
| bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) |
+---------------------------------------------------------+
| 1,2                                                     |
+---------------------------------------------------------+
```
