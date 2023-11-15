# transform_keys

## 功能

对 Map 中的 key 进行 Lambda 转换。有关 Lambda 函数的详细信息，参见 [Lambda expression](../Lambda_expression.md)。

该函数从 3.1 版本开始支持。

## 语法

```Haskell
MAP transform_keys(lambda_func, any_map)
```

`lambda_func` 也可以放在 `any_map` 之后， 即:

```Haskell
MAP transform_keys(any_map, lambda_func)
```

## 参数说明

- `any_map`：要进行运算的 Map 值。

- `lambda_func`：对 `any_map` 的所有 key 进行转换的 Lambda 函数。

## 返回值说明

返回 map。map 的 key 类型由 Lambda 函数的运算结果决定；value 的类型与 `any_map` 中的 value 类型相同。

如果输入参数是 NULL，结果也是 NULL。

如果 MAP 中的某个 key 或 value 是 NULL，该 NULL 值正常处理并返回。

Lambda 函数里必须有两个参数，第一个代表 key，第二个代表 value。

## 示例

以下示例使用 [map_from_arrays](map_from_arrays.md) 生成一个 map 值 `{1:"ab",3:"cdd",2:null,null:"abc"}`。然后将 Lambda 函数应用到 map 中的每个 key，对 key 加 1。

```SQL
mysql> select transform_keys((k,v)->(k+1), col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+------------------------------------------+
| transform_keys((k, v) -> k + 1, col_map) |
+------------------------------------------+
| {2:"ab",4:"cdd",3:null,null:"abc"}       |
+------------------------------------------+
```
