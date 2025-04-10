---
displayed_sidebar: docs
---

# bitmap_count

## Description

Returns the 1-bit count for the input bitmap.

## Syntax

```Haskell
INT BITMAP_COUNT(any_bitmap)
```

## Examples

```Plain Text
MySQL > select bitmap_count(bitmap_from_string("1,2,4"));
+-------------------------------------------+
| bitmap_count(bitmap_from_string('1,2,4')) |
+-------------------------------------------+
|                                         3 |
+-------------------------------------------+

MySQL > select bitmap_count(NULL);
+--------------------+
| bitmap_count(NULL) |
+--------------------+
|                  0 |
+--------------------+
```

## keyword

BITMAP,BITMAP_COUNT
