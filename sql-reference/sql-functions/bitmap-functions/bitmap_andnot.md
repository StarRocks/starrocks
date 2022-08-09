# bitmap_andnot

## 功能

计算两个输入 bitmap 的差集。

## 语法

```Haskell
bitmap_andnot(lhs, rhs)
```

## 参数说明

`lhs`: 支持的数据类型为 BITMAP。

`rhs`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

```plain text
mysql> select bitmap_to_string(bitmap_andnot(bitmap_from_string('1, 3'), bitmap_from_string('2'))) cnt;
+------+
|cnt   |
+------+
|1,3   |
+------+

mysql> select bitmap_to_string(bitmap_andnot(bitmap_from_string('1,3,5'), bitmap_from_string('1'))) cnt;
+------+
|cnt   |
+------+
|3,5   |
+------+
```
