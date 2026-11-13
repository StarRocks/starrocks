---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Calculates whether the input value is in the bitmap column, and returns a Boolean value."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_contains



Calculates whether the input value is in the bitmap column, and returns a Boolean value.

## Syntax

```plaintext
B00LEAN BITMAP_CONTAINS(BITMAP bitmap, BIGINT input)
```

## Examples

```Plain Text
MySQL > select bitmap_contains(to_bitmap(1),2) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_contains(to_bitmap(1),1) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_CONTAINS,BITMAP
