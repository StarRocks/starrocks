---
displayed_sidebar: "Chinese"
---

# map_apply

## 功能

返回 Map 中所有 Key 或 Value 进行 [Lambda](../Lambda_expression.md) 函数运算后的 Map 值。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
MAP map_apply(lambda_func, any_map)
```

## 参数说明

- `lambda_func`: 应用于 Map 的 Lambda 函数。

- `any_map`: 进行 Lambda 函数运算的 Map 值。

## 返回值说明

返回一个 Map。Map 的 Key 和 Value 的数据类型由 Lambda 函数的运算结果决定。

如果输入参数是 NULL，结果也是 NULL。

如果 Map 中的某个 Key 或 Value 是 NULL，该 NULL 值正常计算并返回。

## 示例

以下示例使用 [map_from_arrays](map_from_arrays.md) 生成 map 值 `{1:"ab",3:"cd"}`。然后使用 Lambda 函数对 Map 中的 Key 加 1，对 Value 计算字符长度，比如 "ab" 的字符长度是 2。

```SQL
mysql> select map_apply((k,v)->(k+1,length(v)), col_map) from (select map_from_arrays([1,3],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| {2:2,4:2}                                        |
+--------------------------------------------------+
1 row in set (0.01 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map) from (select map_from_arrays(null,null) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map) from (select map_from_arrays([1,null],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
|{2:2,null:2}                                      |
+--------------------------------------------------+
```
