# bitmap_min

## Description

Obtains the minimum value of a bitmap. If the bitmap is `NULL`, this function returns `NULL`. If the bitmap is empty, this function returns `NULL` by default.

## Syntax

```Haskell
bitmap_min(bitmap)
```

## Parameters

`bitmap`: the bitmap whose minimum value you want to obtain. Only the BITMAP data type is supported. You can specify a bitmap that you construct by using functions such as [bitmap_from_string](bitmap_from_string.md).

## Return value

Returns a value of the LARGEINT data type.

## Examples

```Plain
MySQL > select bitmap_min(bitmap_from_string("0, 1, 2, 3"));
+-------------------------------------------------+
|    bitmap_min(bitmap_from_string('0, 1, 2, 3')) |
+-------------------------------------------------+
|                                               0 |
+-------------------------------------------------+

MySQL > select bitmap_min(bitmap_from_string("-1, 0, 1, 2"));
+-------------------------------------------------+
|   bitmap_min(bitmap_from_string('-1, 0, 1, 2')) |
+-------------------------------------------------+
|                                            NULL |
+-------------------------------------------------+

MySQL > select bitmap_min(bitmap_empty());
+----------------------------------+
|       bitmap_min(bitmap_empty()) |
+----------------------------------+
|                             NULL |
+----------------------------------+

mysql> select bitmap_min(bitmap_from_string("16501189037412846863"));
+--------------------------------------------------------+
| bitmap_min(bitmap_from_string('16501189037412846863')) |
+--------------------------------------------------------+
| 16501189037412846863                                   |
+--------------------------------------------------------+
1 row in set (0.03 sec)
```
