---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列を小文字に変換します。lcase関数と同じです。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# lcase

この関数は文字列を小文字に変換します。lower 関数と同様です。

## Syntax

```plaintext
VARCHAR lcase(VARCHAR str)
```

## Examples

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```

## keyword

LCASE