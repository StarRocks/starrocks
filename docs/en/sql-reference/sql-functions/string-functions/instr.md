---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the 1-based character position of the first occurrence of a substring within a string."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# instr



This function returns the position where str first appeared in substr (start counting from 1 and measured in characters). If str is not found in substr, then this function will return 0.

## Syntax

```plaintext
INT instr(VARCHAR str, VARCHAR substr)
```

## Examples

```Plain Text
MySQL > select instr("abc", "b");
+-------------------+
| instr('abc', 'b') |
+-------------------+
|                 2 |
+-------------------+

MySQL > select instr("abc", "d");
+-------------------+
| instr('abc', 'd') |
+-------------------+
|                 0 |
+-------------------+
```

## keyword

INSTR
