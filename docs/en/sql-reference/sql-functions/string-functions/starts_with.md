---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns 1 if a string starts with the specified prefix, or 0 otherwise."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# starts_with



This function returns 1 when a string starts with a specified prefix. Otherwise, it returns 0. When the argument is NULL, the result is NULL.

## Syntax

```plaintext
BOOLEAN starts_with(VARCHAR str, VARCHAR prefix)
```

## Examples

```Plain Text
mysql> select starts_with("hello world","hello");
+-------------------------------------+
|starts_with('hello world', 'hello')  |
+-------------------------------------+
| 1                                   |
+-------------------------------------+

mysql> select starts_with("hello world","world");
+-------------------------------------+
|starts_with('hello world', 'world')  |
+-------------------------------------+
| 0                                   |
+-------------------------------------+
```

## keyword

START_WITH
