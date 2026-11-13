---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "计算两个 Bitmap 列是否存在相交元素并返回布尔值。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_has_any



计算两个 Bitmap 列是否存在相交元素，返回值是 Boolean 值。

## 语法

```plaintext
BITMAP_HAS_ANY(lhs, rhs)
```

## 参数说明

`lhs`: 支持的数据类型为 BITMAP。

`rhs`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

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
