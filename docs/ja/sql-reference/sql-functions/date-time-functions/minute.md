---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付の分を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# minute

指定された日付の分を返します。返り値は 0 から 59 の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
INT MINUTE(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

MINUTE