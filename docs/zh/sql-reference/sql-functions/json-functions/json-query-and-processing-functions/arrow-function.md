---
displayed_sidebar: "Chinese"
---

# 箭头函数

## 功能

箭头函数 `->` 可以查询 JSON 对象中指定路径（`json_path`）中的值，并输出为 JSON 类型。箭头函数比 JSON_QUERY 函数更简洁易用。

## 语法

```Plain Text
json_object_expr -> json_path
```

## 参数说明

- `json_object_expr`: JSON 对象的表达式，可以是 JSON 类型的列，或者 PARSE_JSON 等 JSON 函数构造的 JSON 对象。

- `json_path`: 查询 JSON 对象时的路径。支持的数据类型为字符串。StarRocks 支持的 JSON Path 的语法，请参见 [JSON Path 语法](../overview-of-json-functions-and-operators.md#json-path)。

## 返回值说明

返回 JSON 类型的值。

> 如果查询的字段不存在，返回 SQL 类型的 NULL。

## 示例

示例一：查询 JSON 对象中路径表达式 `'$.a.b'` 指定的值。

```Plain Text
mysql> SELECT PARSE_JSON('{"a": {"b": 1}}') -> '$.a.b';
       -> 1
```

示例二：支持嵌套使用箭头函数，基于前一个箭头函数的结果进行查询。

> 此示例中的 `json_path` 省略根元素$。

```Plain Text
mysql> SELECT PARSE_JSON('{"a": {"b": 1}}')->'a'->'b';
       -> 1
```

示例三：查询 JSON 对象中路径表达式 `'a'` 指定的值。

> 此示例中的 `json_path` 省略根元素$。

```Plain Text
mysql> SELECT PARSE_JSON('{"a": "b"}') -> 'a';
       -> "b"
```
