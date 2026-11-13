---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "CHAR() returns the character value of the given integer value according to the ASCII table."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# char 



CHAR() returns the character value of the given integer value according to the ASCII table.

## Syntax

```plaintext
char(n)
```

## Parameters

- `n`: integer value

## Return value

Returns a VARCHAR value.

## Examples

```Plain Text
> select char(77);
+----------+
| char(77) |
+----------+
| M        |
+----------+
```

## keyword

CHAR
