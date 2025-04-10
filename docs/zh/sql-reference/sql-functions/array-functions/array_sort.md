---
displayed_sidebar: docs
---

# array_sort

## 功能

对数组中的元素进行升序排列。

## 语法

```Haskell
ARRAY_SORT(array)
```

## 参数说明

`array`：需要排序的数组。支持的数据类型为 ARRAY。

数组元素可以是以下数据类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、DECIMALV2、DATETIME、DATE、JSON。**从 2.5 版本开始，该函数支持 JSON 类型的数组元素。**

## 返回值说明

返回的数据类型为 ARRAY。

## 注意事项

* 只支持升序。
* `null` 会排在前面。
* 如果需要降序排列，可以对排序后的结果，调用 `reverse` 函数。
* 返回的数组元素类型和参数 `array` 中的元素类型一致。

## 示例

下面的示例使用如下数据表进行介绍。

```Plain Text
mysql> select * from test;
+------+--------------+
| c1   | c2           |
+------+--------------+
|    1 | [4,3,null,1] |
|    2 | NULL         |
|    3 | [null]       |
|    4 | [8,5,1,4]    |
+------+--------------+
```

对 `c2` 列数组中的元素进行排序（升序）。

```Plain Text
mysql> select c1, array_sort(c2) from test;
+------+------------------+
| c1   | array_sort(`c2`) |
+------+------------------+
|    1 | [null,1,3,4]     |
|    2 | NULL             |
|    3 | [null]           |
|    4 | [1,4,5,8]        |
+------+------------------+
```
