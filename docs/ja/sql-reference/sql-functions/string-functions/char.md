---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された整数値に対応するASCIIテーブルの文字値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# char

CHAR() は、指定された整数値に対応する ASCII テーブルの文字値を返します。

## 構文

```plaintext
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