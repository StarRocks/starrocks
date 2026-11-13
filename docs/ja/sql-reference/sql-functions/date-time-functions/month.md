---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付の月を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# month

指定された日付の月を返します。戻り値は1から12の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
INT MONTH(DATETIME date)
```

## Examples

```Plain Text
MySQL > select month('1987-01-01');
+-----------------------------+
|month('1987-01-01 00:00:00') |
+-----------------------------+
|                           1 |
+-----------------------------+
```

## keyword

MONTH