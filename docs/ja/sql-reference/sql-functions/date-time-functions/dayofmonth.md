---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "日付から日部分を取得し、1から31の範囲の値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# dayofmonth

日付から日部分を取得し、1 から 31 の範囲の値を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
INT DAYOFMONTH(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```

## keyword

DAYOFMONTH