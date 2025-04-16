---
displayed_sidebar: docs
---

# char

CHAR() は、指定された整数値に対応する ASCII テーブルの文字値を返します。

## 構文

```Haskell
char(n)
```

## パラメータ

- `n`: 整数値

## 戻り値

VARCHAR 値を返します。

## 例

```Plain Text
> select char(77);
+----------+
| char(77) |
+----------+
| M        |
+----------+
```

## キーワード

CHAR