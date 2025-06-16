---
displayed_sidebar: docs
---

# json_exists

检查一个 JSON 对象是否包含可以通过 `json_path` 表达式定位的元素。如果元素存在，JSON_EXISTS 函数返回 `1`。否则，JSON_EXISTS 函数返回 `0`。

:::tip
所有的 JSON 函数和操作符都列在导航栏和[概述页面](../overview-of-json-functions-and-operators.md)上。

通过[生成列](../../../sql-statements/generated_columns.md)加速查询。
:::

## 语法

```Haskell
json_exists(json_object_expr, json_path)
```

## 参数

- `json_object_expr`：表示 JSON 对象的表达式。该对象可以是一个 JSON 列，或是由 JSON 构造函数如 PARSE_JSON 生成的 JSON 对象。

- `json_path`：表示 JSON 对象中元素路径的表达式。此参数的值是一个字符串。有关 StarRocks 支持的 JSON 路径语法的更多信息，请参见 [JSON 函数和操作符概述](../overview-of-json-functions-and-operators.md)。

## 返回值

返回一个 BOOLEAN 值。

## 示例

示例 1：检查指定的 JSON 对象是否包含可以通过 `'$.a.b'` 表达式定位的元素。在此示例中，元素存在于 JSON 对象中。因此，json_exists 函数返回 `1`。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;

       -> 1
```

示例 2：检查指定的 JSON 对象是否包含可以通过 `'$.a.c'` 表达式定位的元素。在此示例中，元素不存在于 JSON 对象中。因此，json_exists 函数返回 `0`。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;

       -> 0
```

示例 3：检查指定的 JSON 对象是否包含可以通过 `'$.a[2]'` 表达式定位的元素。在此示例中，JSON 对象是一个名为 a 的数组，包含索引 2 处的元素。因此，json_exists 函数返回 `1`。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;

       -> 1
```

示例 4：检查指定的 JSON 对象是否包含可以通过 `'$.a[3]'` 表达式定位的元素。在此示例中，JSON 对象是一个名为 a 的数组，不包含索引 3 处的元素。因此，json_exists 函数返回 `0`。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;

       -> 0
```