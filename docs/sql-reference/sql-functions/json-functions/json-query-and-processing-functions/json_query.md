# json_query

## Description

Queries the value of an element that can be located by the `json_path` expression in a JSON object and returns a JSON value.

## Syntax

```Plain_Text
json_query(json_object_expr, json_path)
```

## Parameters

- `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

- `json_path`: the expression that represents the path to an element in the JSON object. The value of this parameter is a string. For more information about the JSON path syntax that is supported by StarRocks, see [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md).

## Return value

Returns a JSON value.

> If the element does not exist, the json_query function returns an SQL value of `NULL`.

## Examples

Example 1: Query the value of an element that can be located by the `'$.a.b'` expression in the specified JSON object. In this example, the json_query function returns a JSON value of `1`.

```Plain_Text
mysql> SELECT json_query(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;

       -> 1
```

Example 2: Query the value of an element that can be located by the `'$.a.c'` expression in the specified JSON object. In this example, the element does not exist. Therefore, the json_query function returns an SQL value of `NULL`.

```Plain_Text
mysql> SELECT json_query(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;

       -> NULL
```

Example 3: Query the value of an element that can be located by the `'$.a[2]'` expression in the specified JSON object. In this example, the JSON object, which is an array named a, contains an element at index 2, and the value of the element is 3. Therefore, the JSON_QUERY function returns a JSON value of `3`.

```Plain_Text
mysql> SELECT json_query(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;

       -> 3
```

Example 4: Query an element that can be located by the `'$.a[3]'` expression in the specified JSON object. In this example, the JSON object, which is an array named a, does not contain an element at index 3. Therefore, the json_query function returns an SQL value of `NULL`.

```Plain_Text
mysql> SELECT json_query(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;

       -> NULL
```
