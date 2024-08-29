---
displayed_sidebar: docs
---

# subdivide_bitmap

## 功能

将大 bitmap 拆成多个子 bitmap。

该函数主要用于将 bitmap 导出，bitmap 太大时，会超出 MySQL 协议的包大小上限。

该函数从 2.5 版本开始支持。

## 语法

```Haskell
BITMAP subdivide_bitmap(bitmap, length)
```

## 参数说明

`bitmap`: 需要拆分的 bitmap，必填。
`length`: 拆分成的大小。每个 bitmap 的长度都需要小于等于这个值, 必填。

## 返回值说明

拆成多个不大于 `length` 的子 bitmap。

## 示例

假设有表 `t1`，其中 `c2` 列为 BITMAP 列。

```Plain
-- 使用 bitmap_to_string() 函数将多行 Bitmap 转换为一个字符串。
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

-- 使用 subdivide_bitmap() 函数将该 Bitmap 拆分成长度不大于 3 的多个 Bitmap。然后使用 bitmap_to_string() 将拆分后的多行 Bitmap 显示到客户端。
mysql> select c1, bitmap_to_string(subdivide_bitmap) from t1, subdivide_bitmap(c2, 3);
+------+------------------------------------+
| c1   | bitmap_to_string(subdivide_bitmap) |
+------+------------------------------------+
|    1 | 1,2,3                              |
|    1 | 4,5,6                              |
|    1 | 7,8,9                              |
|    1 | 10                                 |
+------+------------------------------------+
```
