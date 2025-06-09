---
displayed_sidebar: docs
---

# bitmap_min

## Description

ビットマップの最小値を取得します。ビットマップが `NULL` の場合、この関数は `NULL` を返します。ビットマップが空の場合、この関数はデフォルトで `NULL` を返します。

## Syntax

```Haskell
bitmap_min(bitmap)
```

## Parameters

`bitmap`: 最小値を取得したいビットマップ。BITMAP データ型のみサポートされています。[bitmap_from_string](bitmap_from_string.md) などの関数を使用して構築したビットマップを指定できます。

## Return value

LARGEINT データ型の値を返します。

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