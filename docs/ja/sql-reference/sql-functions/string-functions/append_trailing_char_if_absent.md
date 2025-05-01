---
displayed_sidebar: docs
---

# append_trailing_char_if_absent

## 説明

str 文字列が空でなく、末尾に trailing_char 文字が含まれていない場合、trailing_char 文字を末尾に追加します。trailing_char は 1 文字のみ含むことができます。複数の文字が含まれている場合、この関数は NULL を返します。

## 構文

```Haskell
VARCHAR append_trailing_char_if_absent(VARCHAR str, VARCHAR trailing_char)
```

## 例

```Plain Text
MySQL [test]> select append_trailing_char_if_absent('a','c');
+------------------------------------------+
|append_trailing_char_if_absent('a', 'c')  |
+------------------------------------------+
| ac                                       |
+------------------------------------------+
1 row in set (0.02 sec)

MySQL [test]> select append_trailing_char_if_absent('ac','c');
+-------------------------------------------+
|append_trailing_char_if_absent('ac', 'c')  |
+-------------------------------------------+
| ac                                        |
+-------------------------------------------+
1 row in set (0.00 sec)
```

## キーワード

APPEND_TRAILING_CHAR_IF_ABSENT