# bitmap_min

## Description

Obtains the minimum value of a bitmap. If the bitmap is `NULL`, this function returns `NULL`. If the bitmap is empty, this function returns `-1`.

## Syntax

```SQL
bitmap_min(bitmap)
```

## Parameters

`bitmap`: the bitmap whose minimum value you want to obtain. Only the BITMAP data type is supported. You can specify a bitmap that you construct by using functions such as [bitmap_from_string](https://docs.starrocks.com/zh-cn/2.2/sql-reference/sql-functions/bitmap-functions/bitmap_from_string).

## Return value

Returns a value of the BIGINT data type.

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
|                               -1 |
+----------------------------------+
```
