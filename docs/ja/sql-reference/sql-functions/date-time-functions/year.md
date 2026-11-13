---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "日付の年の部分を1000から9999の範囲の値で返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# year

日付の年の部分を返し、1000 から 9999 の範囲の値を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
INT YEAR(DATETIME date)
```

## Examples

```Plain Text
MySQL > select year('1987-01-01');
+-----------------------------+
| year('1987-01-01 00:00:00') |
+-----------------------------+
|                        1987 |
+-----------------------------+
```

## keyword

YEAR