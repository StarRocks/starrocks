# array_length

## 功能

计算数组中的元素个数，返回值类型是 INT。如果输入参数是 NULL，返回值也是 NULL。数组中的 NULL 元素会计入长度。

该函数别名为 [cardinality](cardinality.md)。

## 语法

```Haskell
INT array_length(any_array)
```

## 参数说明

`any_array`: ARRAY 表达式，必选。

## 返回值说明

返回 INT 类型的值。

## 示例

```plain text
mysql> select array_length([1,2,3]);
+-----------------------+
| array_length([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select array_length([1,2,3,null]);
+-------------------------------+
| array_length([1, 2, 3, NULL]) |
+-------------------------------+
|                             4 |
+-------------------------------+

mysql> select array_length([[1,2], [3,4]]);
+-----------------------------+
| array_length([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## keywords

ARRAY_LENGTH, ARRAY, CARDINALITY
