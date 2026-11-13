---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the number of characters in a string."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# char_length



This function returns the length of a string. For multi-byte characters, it returns the number of characters. It currently only supports utf8 coding. Note: This function is also named as character_length.

## Syntax

```plaintext
INT char_length(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select char_length("abc");
+--------------------+
| char_length('abc') |
+--------------------+
|                  3 |
+--------------------+
```

## keyword

CHAR_LENGTH, CHARACTER_LENGTH
