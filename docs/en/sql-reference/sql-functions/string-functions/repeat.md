---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns a string repeated a specified number of times."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# repeat



This function repeats `str` by a number of times according to `count`. When `count` is below 1, it returns an empty string. When `str` or `count` is NULL, it returns NULL.

## Syntax

```plaintext
VARCHAR repeat(VARCHAR str, INT count)
```

## Examples

```Plain Text
MySQL > SELECT repeat("a", 3);
+----------------+
| repeat('a', 3) |
+----------------+
| aaa            |
+----------------+

MySQL > SELECT repeat("a", -1);
+-----------------+
| repeat('a', -1) |
+-----------------+
|                 |
+-----------------+
```

## keyword

REPEAT,
