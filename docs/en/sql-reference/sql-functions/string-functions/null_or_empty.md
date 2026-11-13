---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns true if the string is NULL or an empty string."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# null_or_empty



This function returns true when the string is empty or NULL. Otherwise, it returns false.

## Syntax

```plaintext
BOOLEAN NULL_OR_EMPTY (VARCHAR str)
BOOLEAN NULL_OR_EMPTY (any_array)
```

## Examples

```Plain Text
MySQL > select null_or_empty(null);
+---------------------+
| null_or_empty(NULL) |
+---------------------+
|                   1 |
+---------------------+

MySQL > select null_or_empty("");
+-------------------+
| null_or_empty('') |
+-------------------+
|                 1 |
+-------------------+

MySQL > select null_or_empty("a");
+--------------------+
| null_or_empty('a') |
+--------------------+
|                  0 |
+--------------------+

MySQL > select null_or_empty([]);
+-------------------+
| null_or_empty('') |
+-------------------+
|                 1 |
+-------------------+

MySQL > select null_or_empty([1]);
+--------------------+
| null_or_empty('a') |
+--------------------+
|                  0 |
+--------------------+
```

## keyword

NULL_OR_EMPTY
