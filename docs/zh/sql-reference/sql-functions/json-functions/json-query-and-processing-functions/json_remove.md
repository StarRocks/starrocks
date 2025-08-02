---
displayed_sidebar: docs
---

# json_remove

从JSON文档中删除一个或多个指定JSON路径的数据，并返回修改后的JSON文档。

:::tip
所有JSON函数和运算符都列在导航和[概述页面](../overview-of-json-functions-and-operators.md)中
:::

## 语法

```Haskell
json_remove(json_object_expr, json_path[, json_path] ...)
```

## 参数

- `json_object_expr`: 表示JSON对象的表达式。对象可以是JSON列，或由JSON构造函数（如PARSE_JSON）生成的JSON对象。

- `json_path`: 一个或多个表示JSON对象中应删除元素路径的表达式。每个参数的值都是字符串。有关StarRocks支持的JSON路径语法信息，请参阅[JSON函数和运算符概述](../overview-of-json-functions-and-operators.md)。

## 返回值

返回删除了指定路径的JSON文档。

> - 如果路径在JSON文档中不存在，则会被忽略。
> - 如果提供了无效路径，则会被忽略。
> - 如果所有路径都无效或不存在，则返回未更改的原始JSON文档。

## 示例

示例1: 从JSON对象中删除单个键。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30]}', '$.a');

       -> {"b": [10, 20, 30]}
```

示例2: 从JSON对象中删除多个键。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30], "c": "test"}', '$.a', '$.c');

       -> {"b": [10, 20, 30]}
```

示例3: 从JSON对象中删除数组元素。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30]}', '$.b[1]');

       -> {"a": 1, "b": [10, 30]}
```

示例4: 删除嵌套对象属性。

```plaintext
mysql> SELECT json_remove('{"a": {"x": 1, "y": 2}, "b": 3}', '$.a.x');

       -> {"a": {"y": 2}, "b": 3}
```

示例5: 尝试删除不存在的路径（被忽略）。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": 2}', '$.c', '$.d');

       -> {"a": 1, "b": 2}
```

示例6: 删除多个路径，包括不存在的路径。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": 2, "c": 3}', '$.a', '$.nonexistent', '$.c');

       -> {"b": 2}
```

## 使用说明

- `json_remove`函数遵循MySQL兼容的行为。
- 无效的JSON路径会被静默忽略，而不是导致错误。
- 该函数支持在单个操作中删除多个路径，这比多个单独操作更高效。
- 目前，该函数支持简单的对象键删除（例如，`$.key`）。复杂嵌套路径和数组元素删除的支持在当前实现中可能有限。 