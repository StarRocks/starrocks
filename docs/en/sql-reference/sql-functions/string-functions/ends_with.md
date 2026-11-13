---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns true if a string ends with a specified suffix."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ends_with



Returns `true` if a string ends with a specified suffix. Otherwise, it returns `false`. If the argument is NULL, the result is NULL.

## Syntax

```plaintext
BOOLEAN ENDS_WITH (VARCHAR str, VARCHAR suffix)
```

## Examples

```Plain Text
MySQL > select ends_with("Hello starrocks", "starrocks");
+-----------------------------------+
| ends_with('Hello starrocks', 'starrocks') |
+-----------------------------------+
|                                 1 |
+-----------------------------------+

MySQL > select ends_with("Hello starrocks", "Hello");
+-----------------------------------+
| ends_with('Hello starrocks', 'Hello') |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

## keyword

ENDS_WITH
