---
displayed_sidebar: docs
---

# Overview of JSON functions and operators

This topic provides an overview of the JSON constructor functions, query functions, and processing functions, operators, and path expressions that are supported by StarRocks.

## JSON constructor functions

JSON constructor functions are used to construct JSON data, such as JSON objects and JSON arrays.

| Function                                                     | Description                                                  | Example                                                   | Return value                           |
| ------------------------------------------------------------ | ------------------------------------------------------------ | --------------------------------------------------------- | -------------------------------------- |
| [json_object](./json-constructor-functions/json_object.md) | Converts one or more key-value pairs to a JSON object that consists of the key-value pairs, which are sorted by key in dictionary order. | `SELECT JSON_OBJECT('Daniel Smith', 26, 'Lily Smith', 25);` | `{"Daniel Smith": 26, "Lily Smith": 25}` |
| [json_array](./json-constructor-functions/json_array.md) | Converts each element of an SQL array to a JSON value and returns a JSON array that consists of those JSON values. | `SELECT JSON_ARRAY(1, 2, 3);`                                | `[1,2,3]`                                |
| [parse_json](./json-constructor-functions/parse_json.md) | Converts a string to a JSON value.                           | `SELECT PARSE_JSON('{"a": 1}');`                             | `{"a": 1}`                               |

## JSON query functions and processing functions

JSON query functions and processing functions are used to query and process JSON data. For example, you can use a path expression to locate an element in a JSON object.

| Function                                                     | Description                                                  | Example                                                    | Return value                                               |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ---------------------------------------------------------- | ---------------------------------------------------------- |
| [arrow function](./json-query-and-processing-functions/arrow-function.md) | Queries the element that can be located by a path expression in a JSON object. | `SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';`                          | `1`                                                          |
| [cast](./json-query-and-processing-functions/cast.md) | Converts data between a JSON data type and an SQL data type. | `SELECT CAST(1 AS JSON);`                       | `1`      |
| [get_json_double](./json-query-and-processing-functions/get_json_double.md)   | Analyzes and gets the floating point value from a specified path in a JSON string.  | `SELECT get_json_double('{"k1":1.3, "k2":"2"}', "$.k1");` | `1.3` |
| [get_json_int](./json-query-and-processing-functions/get_json_int.md)   | Analyzes and gets the integer value from a specified path in a JSON string.  | `SELECT get_json_int('{"k1":1, "k2":"2"}', "$.k1");` | `1` |
| [get_json_string](./json-query-and-processing-functions/get_json_string.md)   | Analyzes and gets the strings from a specified path in a JSON string.  | `SELECT get_json_string('{"k1":"v1", "k2":"v2"}', "$.k1");` | `v1` |
| [json_query](./json-query-and-processing-functions/json_query.md) | Queries the value of an element that can be located by a path expression in a JSON object. | `SELECT JSON_QUERY('{"a": 1}', '$.a');`                         | `1`                                                   |
| [json_each](./json-query-and-processing-functions/json_each.md) | Expands the top-level elements of a JSON object into key-value pairs. | `SELECT * FROM tj_test, LATERAL JSON_EACH(j);` | `!`[json_each](../../../_assets/json_each.png) |
| [json_exists](./json-query-and-processing-functions/json_exists.md) | Checks whether a JSON object contains an element that can be located by a path expression. If the element exists, this function returns 1. If the element does not exist, the function returns 0. | `SELECT JSON_EXISTS('{"a": 1}', '$.a'); `                      | `1`                                     |
| [json_keys](./json-query-and-processing-functions/json_keys.md) | Returns the top-level keys from a JSON object as a JSON array, or, if a path is specified, the top-level keys from the path.   | `SELECT JSON_KEYS('{"a": 1, "b": 2, "c": 3}');` |  `["a", "b", "c"]`|
| [json_length](./json-query-and-processing-functions/json_length.md) | Returns the length of a JSON document.  | `SELECT json_length('{"Name": "Alice"}');` |  `1`  |
| [json_string](./json-query-and-processing-functions/json_string.md)   | Converts the JSON object to a JSON string      | `SELECT json_string(parse_json('{"Name": "Alice"}'));` | `{"Name": "Alice"}`  |

## JSON operators

StarRocks supports the following JSON comparison operators: `<`, `<=`, `>`, `>=`, `=`, and `!=`. You can use these operators to query JSON data. However, it does not allow you to use `IN` to query JSON data. For more information about JSON operators, see [JSON operators](./json-operators.md).

## JSON path expressions

You can use a JSON path expression to query an element in a JSON object. JSON path expressions are of the STRING data type. In most cases, they are used with various JSON functions, such as JSON_QUERY. In StarRocks, JSON path expressions do not completely comply with the [SQL/JSON path specifications](https://modern-sql.com/blog/2017-06/whats-new-in-sql-2016#json-path). For information about the JSON path syntax that is supported in StarRocks, see the following table, in which the following JSON object is used as an example.

```JSON
{
    "people": [{
        "name": "Daniel",
        "surname": "Smith"
    }, {
        "name": "Lily",
        "surname": "Smith",
        "active": true
    }]
}
```

| JSON path symbol | Description                                                  | JSON path example     | Return value                                                 |
| ---------------- | ------------------------------------------------------------ | --------------------- | ------------------------------------------------------------ |
| `$`                | Denotes a root JSON object.                                  | `'$'`                   | `{ "people": [ { "name": "Daniel", "surname": "Smith" }, { "name": "Lily", "surname": Smith, "active": true } ] }`|
|`.`               | Denotes a child JSON object.                                 |`' $.people'`          |`[ { "name": "Daniel", "surname": "Smith" }, { "name": "Lily", "surname": Smith, "active": true } ]`|
|`[]`              | Denotes one or more array indexes. `[n]` denotes the nth element in an array. Indexes start from 0. <br />StarRocks 2.5 supports querying multi-dimensional arrays, for example, `["Lucy", "Daniel"], ["James", "Smith"]`. To query the "Lucy" element, you can use `$.people[0][0]`.| `'$.people [0]'`        | `{ "name": "Daniel", "surname": "Smith" }`                     |
| `[*]`             | Denotes all elements in an array.                            | `'$.people[*].name'`    | `["Daniel", "Lily"]`                                           |
| `[start: end]`     | Denotes a subset of elements from an array. The subset is specified by the `[start, end]` interval, which excludes the element that is denoted by the end index. | `'$.people[0: 1].name'` | `["Daniel"]`                                                   |
