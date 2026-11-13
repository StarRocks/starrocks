---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the ASCII code of the leftmost character of a string."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ascii



This function returns the ascii value of the leftmost character of a given string.

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
