---
displayed_sidebar: docs
---

# to_json

将 Map 或 Struct 值转换为 JSON 字符串。如果输入值为 NULL，则返回 NULL。

:::tip
所有的 JSON 函数和运算符都列在导航栏和[概述页面](../overview-of-json-functions-and-operators.md)上

通过[生成列](../../../sql-statements/generated_columns.md)加速查询
:::

如果您想转换其他数据类型的值，请参阅[cast](./cast.md)。

此函数从 v3.1 开始支持。

## 语法

```Haskell
to_json(any_value)
```

## 参数

`any_value`：您想要转换的 Map 或 Struct 表达式。如果输入值无效，则返回错误。Map 或 Struct 值中的每个键值对的值是可为空的。请参阅最后一个示例。

## 返回值

返回一个 JSON 值。

## 示例

```Haskell
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

## 另请参阅

- [Map 数据类型](../../../data-types/semi_structured/Map.md)
- [Struct 数据类型](../../../data-types/semi_structured/STRUCT.md)
- [Map 函数](../../README.md#map-functions)
- [Struct 函数](../../README.md#struct-functions)