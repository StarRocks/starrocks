# cardinality

## 功能

计算 Map 中元素的个数，返回值类型是 INT。MAP 中保存的是键值对 (key-value pair)，比如 `{"a":1, "b":2}`。一个键值对算作一个元素，`{"a":1, "b":2}` 的元素个数为 2。

该函数从 3.0 版本开始支持。函数别名为 [map_size](map_size.md)。

## 语法

```Haskell
INT cardinality(any_map)
```

## 参数说明

`any_map`: 要获取元素个数的 MAP 值。

## 返回值说明

返回 INT 类型的值。如果输入参数是 NULL，结果也是 NULL。

MAP 中的 Key 和 Value 可以为 NULL，会正常计算。

## 示例

假设 Hive 中有表 `hive_map`，数据如下：

```Plain
SELECT * FROM hive_map ORDER BY col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
3 rows in set (0.05 sec)
```

通过在 StarRocks 集群中[创建 Hive catalog](../../../data_source/catalog/hive_catalog.md#创建-hive-catalog) 来访问该表，计算 `col_map` 列每行的元素个数。

```Plaintext
SELECT cardinality(col_map) FROM hive_map ORDER BY col_int;
+----------------------+
| cardinality(col_map) |
+----------------------+
|                    2 |
|                    1 |
|                    2 |
+----------------------+
3 rows in set (0.05 sec)
```
