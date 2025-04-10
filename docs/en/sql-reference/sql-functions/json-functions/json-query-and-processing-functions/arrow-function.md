---
displayed_sidebar: docs
---

# Arrow function

## Description

Queries an element that can be located by the `json_path` expression in a JSON object and returns a JSON value. The arrow function `->` is more compact and easier to use than the [json_query](json_query.md) function.

## Syntax

```Haskell
json_object_expr -> json_path
```

## Parameters

- `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

- `json_path`: the expression that represents the path to an element in the JSON object. The value of this parameter is a string. For information about the JSON path syntax that is supported by StarRocks, see [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md).

## Return value

Returns a JSON value.

> If the element does not exist, the arrow function returns an SQL value of `NULL`.

## Examples

Example 1: Query an element that can be located by the `'$.a.b'` expression in the specified JSON object.

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';

       -> 1
```

Example 2: Use nested arrow functions to query an element. The arrow function in which another arrow function is nested queries an element based on the result that is returned by the nested arrow function.

> In this example, the root element $ is omitted from the `json_path` expression.

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}')->'a'->'b';

       -> 1
```

Example 3: Query an element that can be located by the `'a'` expression in the specified JSON object.

> In this example, the root element $ is omitted from the `json_path` expression.

```plaintext
mysql> SELECT parse_json('{"a": "b"}') -> 'a';

       -> "b"
```
