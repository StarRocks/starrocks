---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the 1-based position of a string within a comma-separated list."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# find_in_set



This function returns the position of the first str in strlist (start counting with 1). Strlist is a sting separated by commas. If it does not find any str, it returns 0. When the argument is NULL, the result is NULL.

## Syntax

```plaintext
INT find_in_set(VARCHAR str, VARCHAR strlist)
```

## Examples

```Plain Text
MySQL > select find_in_set("b", "a,b,c");
+---------------------------+
| find_in_set('b', 'a,b,c') |
+---------------------------+
|                         2 |
+---------------------------+
```

## keyword

FIND_IN_SET,FIND,IN,SET
