---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Compares two strings and returns 0 if equal, -1 if the first is smaller, or 1 if larger."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# strcmp



This function compares two strings. Returns 0 if lhs and rhs compare equal. Return -1 if lhs appears before rhs in lexicographical order. Returns 1 if lhs appears after rhs in lexicographical order. When the argument is NULL, the result is NULL.

## Syntax

```plaintext
INT strcmp(VARCHAR lhs, VARCHAR rhs)
```

## Examples

```Plain Text
mysql> select strcmp("test1", "test1");
+--------------------------+
| strcmp('test1', 'test1') |
+--------------------------+
|                        0 |
+--------------------------+

mysql> select strcmp("test1", "test2");
+--------------------------+
| strcmp('test1', 'test2') |
+--------------------------+
|                       -1 |
+--------------------------+

mysql> select strcmp("test2", "test1");
+--------------------------+
| strcmp('test2', 'test1') |
+--------------------------+
|                        1 |
+--------------------------+

mysql> select strcmp("test1", NULL);
+-----------------------+
| strcmp('test1', NULL) |
+-----------------------+
|                  NULL |
+-----------------------+
```

## keyword

STRCMP
