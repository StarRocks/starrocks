---
displayed_sidebar: docs
---

# rpad

## 説明

この関数は、`str` の中で最初の音節から数えて `len` の長さの文字列を返します。もし `len` が `str` より長い場合、返り値は `str` の後ろにパッド文字を追加して `len` 文字に伸ばされます。`str` が `len` より長い場合、返り値は `len` 文字に短縮されます。`len` はバイト数ではなく、文字の長さを意味します。

## 構文

```Haskell
VARCHAR rpad(VARCHAR str, INT len, VARCHAR pad)
```

## 例

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
```

## キーワード

RPAD