---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Concatenates multiple strings into a single string."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# concat



This function combines multiple strings. If any of the parameter value is NULL, it will return NULL.

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
