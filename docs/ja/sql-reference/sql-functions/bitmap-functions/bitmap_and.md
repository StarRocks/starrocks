---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "2つの入力ビットマップの交差を計算し、新しいビットマップを返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_and

2 つの入力ビットマップの交差を計算し、新しいビットマップを返します。

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