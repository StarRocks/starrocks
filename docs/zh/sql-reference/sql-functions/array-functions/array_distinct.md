# array_distinct

## 功能

数组元素去重。

## 语法

```Haskell
ARRAY_DISTINCT(array)
```

## 参数说明

`array`：需要去重的数组。支持的数据类型为 ARRAY。

## 返回值说明

返回的数据类型为 ARRAY。

## 注意事项

* 数组中元素不保证顺序。
* 返回数组中的元素类型与 `array` 中的元素类型一致。

## 示例

下面的示例使用如下数据表进行介绍。

```Plain Text
mysql> select * from test;
+------+---------------+
| c1   | c2            |
+------+---------------+
|    1 | [1,1,2]       |
|    2 | [1,null,null] |
|    3 | NULL          |
|    4 | [null]        |
+------+---------------+
```

对 `c2` 列数组中的元素进行去重。

```Plain Text
mysql> select c1, array_distinct(c2) from test;
+------+----------------------+
| c1   | array_distinct(`c2`) |
+------+----------------------+
|    1 | [2,1]                |
|    2 | [null,1]             |
|    3 | NULL                 |
|    4 | [null]               |
+------+----------------------+
```
