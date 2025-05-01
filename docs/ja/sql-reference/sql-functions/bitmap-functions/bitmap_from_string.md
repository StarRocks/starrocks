---
displayed_sidebar: docs
---

# bitmap_from_string

## Description

文字列をBITMAPに変換します。この文字列は、カンマで区切られた一連のUINT64数値で構成されています。例えば、「0, 1, 2」という文字列は、ビット0、1、2が設定されたBitmapに変換されます。入力フィールドが無効な場合、NULLが返されます。

この関数は変換中に入力文字列を重複排除します。他の関数、例えば [bitmap_to_string](bitmap_to_string.md) と組み合わせて、ターミナルで結果を返すために使用されます。

## Syntax

```Haskell
BITMAP BITMAP_FROM_STRING(VARCHAR input)
```

## Return value

BITMAP値を返します。入力文字列が無効な場合、NULLが返されます。入力文字列が空の場合、空の値が返されます。

## Examples

```Plain Text
-- 入力が空で、空の値が返されます。
MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+

-- `0,1,2` が返されます。
MySQL > select bitmap_to_string(bitmap_from_string("0, 1, 2"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 2')) |
+-------------------------------------------------+
| 0,1,2                                           |
+-------------------------------------------------+

-- `-1` は無効な入力であり、NULLが返されます。

MySQL > select bitmap_to_string(bitmap_from_string("-1, 0, 1, 2"));
+-----------------------------------+
| bitmap_from_string('-1, 0, 1, 2') |
+-----------------------------------+
| NULL                              |
+-----------------------------------+

-- 2^64 は無効な入力であり、NULLが返されます。
MySQL > select bitmap_to_string(bitmap_from_string("0, 18446744073709551616"));
+-----------------------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 18446744073709551616')) |
+-----------------------------------------------------------------+
| NULL                                                            |
+-----------------------------------------------------------------+

-- 入力文字列は重複排除されます。

MySQL > select bitmap_to_string(bitmap_from_string("0, 1, 1"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 1')) |
+-------------------------------------------------+
| 0,1                                             |
+-------------------------------------------------+
```

## keywords

BITMAP_FROM_STRING,BITMAP