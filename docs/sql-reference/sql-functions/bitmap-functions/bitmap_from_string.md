# bitmap_from_string

## description

### Syntax

```Haskell
BITMAP BITMAP_FROM_STRING(VARCHAR input)
```

Convert a string into a BITMAP. The string is composed of a set of UINT32 numbers separated by commas. For example, "0, 1, 2" string will be converted into a Bitmap, in which bits 0, 1 and 2 are set. When the input field is illegal, NULL will be returned.

## example

```Plain Text
MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+

MySQL > select bitmap_to_string(bitmap_from_string("0, 1, 2"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 2')) |
+-------------------------------------------------+
| 0,1,2                                           |
+-------------------------------------------------+

MySQL > select bitmap_from_string("-1, 0, 1, 2");
+-----------------------------------+
| bitmap_from_string('-1, 0, 1, 2') |
+-----------------------------------+
| NULL                              |
+-----------------------------------+
```

## keyword

BITMAP_FROM_STRING,BITMAP
