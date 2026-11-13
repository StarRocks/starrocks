---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Calculates the set consisting elements unique to lhs and rhs."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_xor



Calculates the set consisting elements unique to `lhs` and `rhs`. It is logically equivalent to `bitmap_andnot(bitmap_or(lhs, rhs), bitmap_and(lhs, rhs))` (complementary set).

## Syntax

```plaintext
bitmap_xor(BITMAP lhs, BITMAP rhs)
```

## Examples

```plain text
mysql> select bitmap_to_string(bitmap_xor(bitmap_from_string('1, 3'), bitmap_from_string('2'))) cnt;
+------+
|cnt   |
+------+
|1,2,3 |
+------+
```

## keyword

BITMAP_XOR,  BITMAP
