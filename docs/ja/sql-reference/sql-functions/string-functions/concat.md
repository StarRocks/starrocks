---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "複数の文字列を1つの文字列に結合します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# concat

この関数は複数の文字列を結合します。パラメータの値が NULL の場合、NULL を返します。

## Syntax

```plaintext
VARCHAR concat(VARCHAR,...)
```

## Examples

```Plain Text
MySQL > select concat("a", "b");
+------------------+
| concat('a', 'b') |
+------------------+
| ab               |
+------------------+

MySQL > select concat("a", "b", "c");
+-----------------------+
| concat('a', 'b', 'c') |
+-----------------------+
| abc                   |
+-----------------------+

MySQL > select concat("a", null, "c");
+------------------------+
| concat('a', NULL, 'c') |
+------------------------+
| NULL                   |
+------------------------+
```

## keyword

CONCAT