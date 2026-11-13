---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付の時間を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hour

指定された日付の時間を返します。返される値は 0 から 23 の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
INT HOUR(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select hour('2018-12-31 23:59:59');
+-----------------------------+
| hour('2018-12-31 23:59:59') |
+-----------------------------+
|                          23 |
+-----------------------------+
```

## keyword

HOUR