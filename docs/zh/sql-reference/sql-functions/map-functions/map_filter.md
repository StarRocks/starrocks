# map_filter

## 功能

根据设定的过滤函数返回 MAP 中匹配的 Key-value 对。该过滤函数可以是普通的 Boolean 数组，也可以是灵活的 Lambda 函数。有关 Lambda 函数的详细信息，参见 [Lambda expression](../Lambda_expression.md)。

该函数从 3.1 版本开始支持。

## 语法

```Haskell
MAP map_filter(any_map, array<boolean>)
MAP map_filter(lambda_func, any_map)
```

- `map_filter(any_map, array<boolean>)`

  返回 `any_map` 中与 `array<boolean>` 一一对应位置为 `true` 的 key-value 对。

- `map_filter(lambda_func, any_map)`

  返回 `any_map` 中应用 `lambda_func` 后值为 `true` 的 key-value 对。

## 参数说明

- `any_map`: 要进行过滤运算的 MAP 值。

- `array<boolean>`: 对 `any_map` 进行过滤的 Boolean 数组。

- `lambda_func`: 对 `any_map` 进行过滤的 Lambda 函数。

## 返回值说明

返回 map, 类型与输入的 `any_map` 相同。

如果 `any_map` 是 NULL，结果也是 NULL; 如果 `array<boolean>` 是 null，结果是空 MAP。

如果 Map 中的某个 Key 或 Value 是 NULL，该 NULL 值正常参与计算并返回。

Lambda 函数里必须有两个参数，第一个代表 Key，第二个代表 Value。

## 示例

### 不使用 lambda 表达式

以下示例使用 [map_from_arrays](map_from_arrays.md) 生成一个 map 值 `{1:"ab",3:"cdd",2:null,null:"abc"}`。然后将 `array<boolean>` 应用到 Map 里的每个 key-value 对，返回为结果 true 的 key-value 对。

```SQL
mysql> select map_filter(col_map, array<boolean>[0,0,0,1,1]) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+----------------------------------------------------+
| map_filter(col_map, ARRAY<BOOLEAN>[0, 0, 0, 1, 1]) |
+----------------------------------------------------+
| {null:"abc"}                                       |
+----------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_filter(null, array<boolean>[0,0,0,1,1]);
+-------------------------------------------------+
| map_filter(NULL, ARRAY<BOOLEAN>[0, 0, 0, 1, 1]) |
+-------------------------------------------------+
| NULL                                            |
+-------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_filter(col_map, null) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+---------------------------+
| map_filter(col_map, NULL) |
+---------------------------+
| {}                        |
+---------------------------+
1 row in set (0.01 sec)
```

### 使用 Lambda 表达式

以下示例使用 map_from_arrays() 生成一个 Map 值 `{1:"ab",3:"cdd",2:null,null:"abc"}`。然后将 Lambda 函数应用到 Map 中的每个 key-value 对，返回 Value 非 null 的 key-value 对。

```SQL
mysql> select map_filter((k,v) -> v is not null,col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+------------------------------------------------+
| map_filter((k,v) -> v is not null, col_map)    |
+------------------------------------------------+
| {1:"ab",3:"cdd",null:'abc'}                    |
+------------------------------------------------+
1 row in set (0.02 sec)
```
