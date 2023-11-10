# element_at

## 功能

获取 Array 数组中指定位置的元素。如果输入值为 NULL 或指定的位置不存在，则返回 NULL。

该函数是数组下标 `[]` 操作符的别名。

如果您想从 MAP 数据中查询指定 Key 对应的 Value，参见 Map 函数中的 [element_at](../map-functions/element_at.md)。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
element_at(any_array, position)
```

## 参数说明

- `any_array`: ARRAY 表达式。
- `position`: 数组下标，从 1 开始。必须是正整数，取值不能超过数组的长度。如果 `position` 不存在，返回 NULL。

## 返回值说明

返回 `position` 对应位置的元素。

## 示例

```plain text
mysql> select element_at([2,3,11],3);
+-----------------------+
|  element_at([11,2,3]) |
+-----------------------+
|                    11 |
+-----------------------+

mysql> select element_at(["a","b","c"],1);
+--------------------+
| ['a', 'b', 'c'][1] |
+--------------------+
| a                  |
+--------------------+
```

## keyword

ELEMENT_AT, ARRAY
