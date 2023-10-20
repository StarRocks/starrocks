# json_exists

## Description

Checks whether a JSON object contains an element that can be located by the `json_path` expression. If the element exists, the JSON_EXISTS function returns `1`. Otherwise, the JSON_EXISTS function returns `0`.

## Syntax

```Plain_Text
json_exists(json_object_expr, json_path)
```

## Parameters

- `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

- `json_path`: the expression that represents the path to an element in the JSON object. The value of this parameter is a string. For information about the JSON path syntax that is supported by StarRocks, see [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md).

## Return value

Returns a BOOLEAN value.

## Examples

Example 1: Check whether the specified JSON object contains an element that can be located by the `'$.a.b'` expression. In this example, the element exists in the JSON object. Therefore, the json_exists function returns `1`.

```Plain_Text
mysql> SELECT json_exists(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;

       -> 1
```

Example 2: Check whether the specified JSON object contains an element that can be located by the `'$.a.c'` expression. In this example, the element does not exist in the JSON object. Therefore, the json_exists function returns `0`.

```Plain_Text
mysql> SELECT json_exists(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;

       -> 0
```

Example 3: Check whether the specified JSON object contains an element that can be located by the `'$.a[2]'` expression. In this example, the JSON object, which is an array named a, contains an element at index 2. Therefore, the json_exists function returns `1`.

```Plain_Text
mysql> SELECT json_exists(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;

       -> 1
```

Example 4: Check whether the specified JSON object contains an element that can be located by the `'$.a[3]'` expression. In this example, the JSON object, which is an array named a, does not contain an element at index 3. Therefore, the json_exists function returns `0`.

```Plain_Text
mysql> SELECT json_exists(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;

       -> 0
```
