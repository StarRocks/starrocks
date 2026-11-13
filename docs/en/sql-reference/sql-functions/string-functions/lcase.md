---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a string to lowercase; alias of lower."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# lcase



This function converts a string to lower-case. It is analogous to the function lower.

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
