# json_update

Updates a JSON object by setting the value at the specified path and returns the modified JSON object.

tip

All of the JSON functions and operators are listed in the navigation and on the overview page

Accelerate your queries with generated columns

## Syntax

```
json_update(json_object_expr, json_path, new_value)
```

## Parameters

* `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.
* `json_path`: the expression that represents the path to an element in the JSON object. The value of this parameter is a string. For information about the JSON path syntax that is supported by StarRocks, see Overview of JSON functions and operators.
* `new_value`: the new JSON value to set at the specified path. This can be any valid JSON value.

## Return value

Returns a JSON object with the value at the specified path updated to the new value.

> If the path does not exist, the function will create the necessary structure to set the value at that path.
> If any of the input parameters is NULL, the function returns NULL.

## Examples

Example 1: Update a simple key in a JSON object.

```sql
mysql> SELECT json_update(PARSE_JSON('{"a": 1, "b": 2}'), 'a', PARSE_JSON('42'));

       -> {"a": 42, "b": 2}
```

Example 2: Update a nested value in a JSON object.

```sql
mysql> SELECT json_update(PARSE_JSON('{"a": {"b": 1, "c": 2}, "d": 3}'), 'a.b', PARSE_JSON('99'));

       -> {"a": {"b": 99, "c": 2}, "d": 3}
```

Example 3: Update an array element.

```sql
mysql> SELECT json_update(PARSE_JSON('{"arr": [1, 2, 3]}'), 'arr[1]', PARSE_JSON('99'));

       -> {"arr": [1, 99, 3]}
```

Example 4: Add a new key to a JSON object.

```sql
mysql> SELECT json_update(PARSE_JSON('{"a": 1}'), 'b', PARSE_JSON('"new_value"'));

       -> {"a": 1, "b": "new_value"}
```

Example 5: Update using a JSON path with root notation.

```sql
mysql> SELECT json_update(PARSE_JSON('{"a": 1, "b": 2}'), '$.a', PARSE_JSON('100'));

       -> {"a": 100, "b": 2}
```

## Usage notes

* The function supports both simple path notation (e.g., `'a.b'`) and JSON path notation with root symbol (e.g., `'$.a.b'`).
* Array indices are specified using square brackets (e.g., `'arr[0]'`).
* If the specified path doesn't exist, the function will create the necessary intermediate objects.
* The function preserves the original structure of the JSON object, only modifying the specified path.
* This function is useful for updating specific fields in JSON documents stored in tables without having to reconstruct the entire JSON object.