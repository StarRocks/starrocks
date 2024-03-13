---
displayed_sidebar: "English"
---

# bitmap_from_string

## Description

Converts a string into a BITMAP. The string is composed of a set of UINT32 numbers separated by commas. For example, the "0, 1, 2" string will be converted into a Bitmap, in which bits 0, 1 and 2 are set. If the input field is invalid, NULL will be returned.

This function deduplicates the input string during the conversion. It must be used with other functions, such as [bitmap_to_string](bitmap_to_string.md).

## Syntax

```Haskell
BITMAP BITMAP_FROM_STRING(VARCHAR input)
```

## Examples

```Plain Text

-- The input is empty and an empty value is returned.

MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+

-- `0,1,2` is returned.

MySQL > select bitmap_to_string(bitmap_from_string("0, 1, 2"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 2')) |
+-------------------------------------------------+
| 0,1,2                                           |
+-------------------------------------------------+

-- `-1` is an invalid input and NULL is returned.

MySQL > select bitmap_to_string(bitmap_from_string("-1, 0, 1, 2"));
+-----------------------------------+
| bitmap_from_string('-1, 0, 1, 2') |
+-----------------------------------+
| NULL                              |
+-----------------------------------+

-- The input string is deduplicated.

MySQL > select bitmap_to_string(bitmap_from_string("0, 1, 1"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 1')) |
+-------------------------------------------------+
| 0,1                                             |
+-------------------------------------------------+
```

## keywords

BITMAP_FROM_STRING,BITMAP
