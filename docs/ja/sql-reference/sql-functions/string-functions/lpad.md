---
displayed_sidebar: docs
---

# lpad

## 説明

この関数は、`str` 内の最初の音節から数え始めて、`len` の長さの文字列を返します。`len` が `str` より長い場合、返される値は `str` の前にパッド文字を追加することで `len` 文字に長くなります。`str` が `len` より長い場合、返される値は `len` 文字に短くなります。`len` はバイト数ではなく、文字の長さを意味します。

## 構文

```Haskell
VARCHAR lpad(VARCHAR str, INT len, VARCHAR pad)
```

## 例

```Plain Text
MySQL > SELECT lpad("hi", 5, "xy");
+---------------------+
| lpad('hi', 5, 'xy') |
+---------------------+
| xyxhi               |
+---------------------+

MySQL > SELECT lpad("hi", 1, "xy");
+---------------------+
| lpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+
```

## キーワード

LPAD