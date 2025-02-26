---
displayed_sidebar: docs
---

# rpad

この関数は、`str` の最初の音節から数えて `len` の長さの文字列を返します。`len` が `str` より長い場合、返される値は `str` の後ろにパッド文字を追加して `len` 文字に伸ばされます。`str` が `len` より長い場合、返される値は `len` 文字に短縮されます。`len` はバイト数ではなく、文字数を意味します。

## Syntax

```Haskell
VARCHAR rpad(VARCHAR str, INT len[, VARCHAR pad])
```

## Parameters

`str`: 必須、パディングされる文字列で、VARCHAR 値に評価される必要があります。

`len`: 必須、返される値の長さで、バイト数ではなく文字数を意味し、INT 値に評価される必要があります。

`pad`: オプション、`str` の後ろに追加される文字で、VARCHAR 値である必要があります。このパラメータが指定されていない場合、デフォルトでスペースが追加されます。

## Return value

VARCHAR 値を返します。

## Examples

```Plain Text
MySQL > SELECT rpad("hi", 5, "xy");
+---------------------+
| rpad('hi', 5, 'xy') |
+---------------------+
| hixyx               |
+---------------------+

MySQL > SELECT rpad("hi", 1, "xy");
+---------------------+
| rpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+

MySQL > SELECT rpad("hi", 5);
+---------------------+
| rpad('hi', 5, ' ')  |
+---------------------+
| hi                  |
+---------------------+
```

## keyword

RPAD