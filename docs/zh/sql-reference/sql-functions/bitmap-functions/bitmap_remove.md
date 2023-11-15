# bitmap_remove

## 功能

从 Bitmap 中删除指定的数值。

## 语法

```Haskell
bitmap_remove(lhs, input)
```

## 参数说明

`lhs`: 支持的数据类型为 BITMAP。

`input`: 支持的数据类型为 BIGINT。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

```plain text
mysql> select bitmap_to_string(bitmap_remove(bitmap_from_string('1, 3'), 3)) cnt;
+------+
|cnt   |
+------+
|1     |
+------+

mysql> select bitmap_to_string(bitmap_remove(bitmap_from_string('1,3,5'), 6)) cnt;
+------+
|cnt   |
+------+
|1,3,5 |
+------+
```
