---
displayed_sidebar: docs
---

# instr

## 説明

この関数は、`str` が `substr` に初めて現れる位置を返します（1から数え始め、文字単位で測定）。`str` が `substr` に見つからない場合、この関数は0を返します。

## 構文

```Haskell
INT instr(VARCHAR str, VARCHAR substr)
```

## 例

```Plain Text
MySQL > select instr("abc", "b");
+-------------------+
| instr('abc', 'b') |
+-------------------+
|                 2 |
+-------------------+

MySQL > select instr("abc", "d");
+-------------------+
| instr('abc', 'd') |
+-------------------+
|                 0 |
+-------------------+
```

## キーワード

INSTR