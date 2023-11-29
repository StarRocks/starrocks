---
displayed_sidebar: "Chinese"
---

# to_json

## 功能

将 Map 或 Struct 类型的数据转换成 JSON 数据。

如果要转换其他类型的数据，参见 [cast](./cast.md)。该函数从 3.1 版本开始支持。

## 语法

```Haskell
to_json(any_value)
```

## 参数说明

`any_value`: 必须是 Map 或 Struct 类型的数值或表达式，否则返回报错。如果 `any_value` 为 Null，则返回 Null。Map 和 Struct 内的元素 Value 可以是 Null，会正常返回。参见最后一个示例。

## 返回值说明

返回 JSON 类型的值。

## 示例

```Haskell
select to_json(map_from_arrays([1, 2], ['Star', 'Rocks']));
+-----------------------------------------------------+
| to_json(map_from_arrays([1, 2], ['Star', 'Rocks'])) |
+-----------------------------------------------------+
| {"1": "Star", "2": "Rocks"}                         |
+-----------------------------------------------------+

select to_json(map{1:'a',2:'b'});
+---------------------------+
| to_json(map{1:'a',2:'b'}) |
+---------------------------+
| {"1": "a", "2": "b"}      |
+---------------------------+

select to_json(row('asia','eu'));
+--------------------------------+
| to_json(row('asia', 'eu'))     |
+--------------------------------+
| {"col1": "asia", "col2": "eu"} |
+--------------------------------+

select to_json(map('a', named_struct('b', 1)));
+----------------------------------------+
| to_json(map{'a':named_struct('b', 1)}) |
+----------------------------------------+
| {"a": {"b": 1}}                        |
+----------------------------------------+

select to_json(named_struct("k1", cast(null as string), "k2", "v2"));
+-----------------------------------------------------------------------+
| to_json(named_struct('k1', CAST(NULL AS VARCHAR(65533)), 'k2', 'v2')) |
+-----------------------------------------------------------------------+
| {"k1": null, "k2": "v2"}                                              |
+-----------------------------------------------------------------------+
```

## 相关文档

- [Map 数据类型](../../../sql-statements/data-types/Map.md)
- [Struct 数据类型](../../../sql-statements/data-types/STRUCT.md)
- [Map 函数](../../function-list.md#map-函数)
- [Struct 函数](../../function-list.md#struct-函数)
