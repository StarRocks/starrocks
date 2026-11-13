---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Removes input from the Bitmap value lhs and returns a result set."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_remove



Removes `input` from the Bitmap value `lhs` and returns a result set.

## Syntax

```plaintext
bitmap_remove(BITMAP lhs, BIGINT input)
```

## Examples

```plain text
mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1, 3'), 3)) cnt;
+------+
|cnt   |
+------+
|1     |
+------+

mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1,3,5'), 6)) cnt;
+------+
|cnt   |
+------+
|1,3,5 |
+------+
```

## keyword

BITMAP_REMOVE, BITMAP
