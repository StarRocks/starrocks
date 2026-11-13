---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Calculates the intersection of two input bitmaps and returns the new bitmap."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_and



Calculates the intersection of two input bitmaps and returns the new bitmap.

## Syntax

```plaintext
BITMAP BITMAP_AND(BITMAP lhs, BITMAP rhs)
```

## Examples

```plain text
MySQL > select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(2))) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(1))) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_AND,BITMAP
