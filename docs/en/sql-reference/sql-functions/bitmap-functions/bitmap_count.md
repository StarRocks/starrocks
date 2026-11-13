---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the 1-bit count for the input bitmap."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_count



Returns the 1-bit count for the input bitmap.

## Syntax

```plaintext
INT BITMAP_COUNT(any_bitmap)
```

## Examples

```Plain Text
MySQL > select bitmap_count(bitmap_from_string("1,2,4"));
+-------------------------------------------+
| bitmap_count(bitmap_from_string('1,2,4')) |
+-------------------------------------------+
|                                         3 |
+-------------------------------------------+

MySQL > select bitmap_count(NULL);
+--------------------+
| bitmap_count(NULL) |
+--------------------+
|                  0 |
+--------------------+
```

## keyword

BITMAP,BITMAP_COUNT
