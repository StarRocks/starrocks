---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "入力されたビットマップの1ビット数を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_count

入力されたビットマップの 1 ビット数を返します。

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