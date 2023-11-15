# cardinality

## 功能

计算数组中的元素个数，返回值类型是 INT。如果输入参数是 NULL，返回值也是 NULL。数组中的 NULL 元素会计入长度，比如 `[1,2,3,null]` 会计算为 4 个元素。

该函数从 3.0 版本开始支持。别名为 [array_length()](array_length.md)。

## 语法

```Haskell
INT cardinality(any_array)
```

## 参数说明

`any_array`: ARRAY 表达式，必选。

## 返回值说明

返回 INT 类型的元素个数。

## 示例

```plain text
mysql> select cardinality([1,2,3]);
+-----------------------+
|  cardinality([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select cardinality([1,2,3,null]);
+------------------------------+
| cardinality([1, 2, 3, NULL]) |
+------------------------------+
|                            4 |
+------------------------------+

mysql> select cardinality([[1,2], [3,4]]);
+-----------------------------+
|  cardinality([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## keywords

CARDINALITY, ARRAY_LENGTH, ARRAY
