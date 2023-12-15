---
displayed_sidebar: "Chinese"
---

# subdivide_bitmap

## 功能

将大 bitmap 拆成多个子 bitmap。

该函数主要用于将 bitmap 导出，当 bitmap 太大时，会超出 Mysql 协议的包大小上限。

该函数从 2.5 版本开始支持

## 语法

```Haskell
BITMAP subdivide_bitmap(bitmap, length)
```

## 参数说明

`bitmap`: 需要拆分的 bitmap，必填。
`length`: 拆分的大小，大于这个值的 bitmap 会拆成多个小 bitmap, 每个 bitmap 的 Size 都小于这个值, 必填。

## 返回值说明

拆成多个不大于 length 的子 bitmap。

## 示例

```Plain
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

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