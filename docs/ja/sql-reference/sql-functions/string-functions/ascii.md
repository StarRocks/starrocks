---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列の最左の文字のASCII値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ascii

この関数は、指定された文字列の最左の文字の ASCII 値を返します。

## Syntax

```plaintext
INT ascii(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select ascii('1');
+------------+
| ascii('1') |
+------------+
|         49 |
+------------+

MySQL > select ascii('234');
+--------------+
| ascii('234') |
+--------------+
|           50 |
+--------------+
```

## keyword

ASCII