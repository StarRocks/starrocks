---
displayed_sidebar: docs
---

# json_contains

检查一个 JSON 文档是否包含特定的值或子文档。如果目标 JSON 文档包含候选 JSON 值，JSON_CONTAINS 函数返回 `1`。否则，JSON_CONTAINS 函数返回 `0`。

:::tip
所有的 JSON 函数和操作符都列在导航栏和[概述页面](../overview-of-json-functions-and-operators.md)上。

:::

## 语法

```Haskell
json_contains(json_target, json_candidate)
```

## 参数

- `json_target`：表示目标 JSON 文档的表达式。该文档可以是一个 JSON 列，或是由 JSON 构造函数如 PARSE_JSON 生成的 JSON 对象。

- `json_candidate`：表示要在目标中搜索的候选 JSON 值或子文档的表达式。该值可以是一个 JSON 列，或是由 JSON 构造函数如 PARSE_JSON 生成的 JSON 对象。

## 返回值

返回一个 BOOLEAN 值。

## 使用说明

- 对于标量值（字符串、数字、布尔值、null），如果值相等则函数返回 true。
- 对于 JSON 对象，如果目标对象包含候选对象的所有键值对，则函数返回 true。
- 对于 JSON 数组，如果目标数组包含候选数组的所有元素，或者候选是包含在目标数组中的单个值，则函数返回 true。
- 该函数对嵌套结构执行深度包含检查。

## 示例

示例 1：检查 JSON 对象是否包含特定的键值对。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"a": 1}'));

       -> 1
```

示例 2：检查 JSON 对象是否包含不存在的键值对。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"c": 3}'));

       -> 0
```

示例 3：检查 JSON 数组是否包含特定元素。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('[2, 3]'));

       -> 1
```

示例 4：检查 JSON 数组是否包含单个标量值。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('2'));

       -> 1
```

示例 5：检查 JSON 数组是否包含不存在的元素。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('[5, 6]'));

       -> 0
```

示例 6：检查嵌套 JSON 结构的包含关系。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"users": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]}'), 
                           PARSE_JSON('{"users": [{"id": 1}]}'));

       -> 0
```

注意：在最后一个示例中，结果是 0，因为数组包含需要精确的元素匹配，而不是数组内对象的部分匹配。