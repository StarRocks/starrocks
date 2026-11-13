---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付の年内の週番号を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# weekofyear

指定された日付の年内の週番号を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
INT WEEKOFYEAR(DATETIME date)
```

## 例

```Plain Text
MySQL > select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## キーワード

WEEKOFYEAR