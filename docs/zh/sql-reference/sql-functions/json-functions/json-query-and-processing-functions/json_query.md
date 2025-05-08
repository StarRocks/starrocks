---
displayed_sidebar: docs
---

# json_query

查询可以通过 `json_path` 表达式在 JSON 对象中定位的元素的值，并返回一个 JSON 值。

## 语法

```Haskell
json_query(json_object_expr, json_path)
```

## 参数

- `json_object_expr`：表示 JSON 对象的表达式。该对象可以是一个 JSON 列，或者是由 JSON 构造函数（如 PARSE_JSON）生成的 JSON 对象。

- `json_path`：表示 JSON 对象中元素路径的表达式。此参数的值是一个字符串。有关 StarRocks 支持的 JSON 路径语法的信息，请参见 [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md)。

## 返回值

返回一个 JSON 值。

> 如果元素不存在，json_query 函数返回 SQL 值 `NULL`。

## 示例

示例 1：查询可以通过 `'$.a.b'` 表达式在指定 JSON 对象中定位的元素的值。在此示例中，json_query 函数返回 JSON 值 `1`。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;

       -> 1
```

示例 2：查询可以通过 `'$.a.c'` 表达式在指定 JSON 对象中定位的元素的值。在此示例中，元素不存在。因此，json_query 函数返回 SQL 值 `NULL`。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;

       -> NULL
```

示例 3：查询可以通过 `'$.a[2]'` 表达式在指定 JSON 对象中定位的元素的值。在此示例中，JSON 对象是一个名为 a 的数组，包含索引 2 处的元素，元素的值为 3。因此，JSON_QUERY 函数返回 JSON 值 `3`。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;

       -> 3
```

示例 4：查询可以通过 `'$.a[3]'` 表达式在指定 JSON 对象中定位的元素。在此示例中，JSON 对象是一个名为 a 的数组，不包含索引 3 处的元素。因此，json_query 函数返回 SQL 值 `NULL`。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;

       -> NULL
```

:::tip
所有的 JSON 函数和操作符都列在导航栏和 [overview page](../overview-of-json-functions-and-operators.md) 上。
:::