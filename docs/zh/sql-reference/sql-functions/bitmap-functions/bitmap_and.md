---
displayed_sidebar: docs
---

# bitmap_and



计算多个 bitmap 的交集，返回新的 bitmap。

## 语法

```Haskell
BITMAP_AND(bm, bm [,...])
```

## 参数说明

`bm`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

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
