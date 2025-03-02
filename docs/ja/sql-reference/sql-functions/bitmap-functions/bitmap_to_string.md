---
displayed_sidebar: docs
---

# bitmap_to_string

入力されたビットマップをカンマ (,) で区切られた文字列に変換します。この文字列にはビットマップ内のすべてのビットが含まれます。入力が null の場合、null が返されます。

## Syntax

```Haskell
VARCHAR BITMAP_TO_STRING(BITMAP input)
```

## Parameters

`input`: 変換したいビットマップ。

## Return value

VARCHAR 型の値を返します。

## Examples

例 1: 入力が null の場合、null が返されます。

```Plain Text
MySQL > select bitmap_to_string(null);
+------------------------+
| bitmap_to_string(NULL) |
+------------------------+
| NULL                   |
+------------------------+
```

例 2: 入力が空の場合、空の文字列が返されます。

```Plain Text
MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+
```

例 3: 1 ビットを含むビットマップを文字列に変換します。

```Plain Text
MySQL > select bitmap_to_string(to_bitmap(1));
+--------------------------------+
| bitmap_to_string(to_bitmap(1)) |
+--------------------------------+
| 1                              |
+--------------------------------+
```

例 4: 2 ビットを含むビットマップを文字列に変換します。

```Plain Text
MySQL > select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));
+---------------------------------------------------------+
| bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) |
+---------------------------------------------------------+
| 1,2                                                     |
+---------------------------------------------------------+
```