---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Calculates the union of two input bitmaps and return a new bitmap."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_or



Calculates the union of two input bitmaps and return a new bitmap.

## Syntax

```plaintext
BITMAP BITMAP_OR(BITMAP lhs, BITMAP rhs)
```

## Examples

```Plain Text
MySQL > select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(2))) cnt;
+------+
| cnt  |
+------+
|    2 |
+------+

MySQL > select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(1))) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_OR,BITMAP
