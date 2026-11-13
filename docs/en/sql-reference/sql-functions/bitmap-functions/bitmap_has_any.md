---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Calculates whether there are intersecting elements between two Bitmap columns, and the return value is Boolean value."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_has_any



Calculates whether there are intersecting elements between two Bitmap columns, and the return value is Boolean value.

## Syntax

```plaintext
B00LEAN BITMAP_HAS_ANY(BITMAP lhs, BITMAP rhs)
```

## Examples

```Plain Text
MySQL > select bitmap_has_any(to_bitmap(1),to_bitmap(2)) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_has_any(to_bitmap(1),to_bitmap(1)) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_HAS_ANY,BITMAP
