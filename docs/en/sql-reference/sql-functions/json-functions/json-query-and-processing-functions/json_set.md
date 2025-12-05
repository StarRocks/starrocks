---
displayed_sidebar: docs
---

# json_set

Inserts or updates data in a JSON document at one or more specified JSON paths and returns the modified JSON document.

:::tip
All of the JSON functions and operators are listed in the navigation and on the overview page.
:::

## Syntax

```SQL
json_set(json_object_expr, json_path, value[, json_path, value] ...)
```

## Parameters
- `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as **PARSE_JSON**.

- `json_path`: the path to the element in the JSON object that you want to insert or update. The value must be a string. For information about the JSON path syntax that is supported by StarRocks, see Overview of JSON functions and operators.

- `value`: the value to be inserted or updated at the specified path. It can be a string, number, boolean, null, or a JSON object.

## Return value
Returns the modified JSON document.

> - Returns *NULL* if any argument is *NULL*.
> - If the path exists in the JSON document, the existing value is updated (replaced).
> - If the path does not exist, the new value is inserted (Upsert behavior).
> - Arguments are evaluated from left to right. The result of the first path-value pair becomes the input for the second pair.

## Examples

Example 1: Insert a new key into a JSON object.

```Plaintext
mysql> SELECT json_set('{"a": 1}', '$.b', 2);
       -> {"a": 1, "b": 2}
```

Example 2: Update an existing key in a JSON object.

```Plaintext
mysql> SELECT json_set('{"a": 1}', '$.a', 10);
       -> {"a": 10}
```

Example 3: Perform multiple operations (Update one existing key, Insert one new key).

```Plaintext
mysql> SELECT json_set('{"a": 1, "b": 2}', '$.a', 10, '$.c', 3);
       -> {"a": 10, "b": 2, "c": 3}
```

Example 4: Update a value inside a nested JSON object.
```Plaintext

mysql> SELECT json_set('{"a": {"x": 1, "y": 2}}', '$.a.x', 100);
       -> {"a": {"x": 100, "y": 2}}
```

Example 5: Update an element in an array by index.

```Plaintext
mysql> SELECT json_set('{"arr": [10, 20, 30]}', '$.arr[1]', 99);
       -> {"arr": [10, 99, 30]}
```

Example 6: Append to an array (using an index larger than the array length).

```Plaintext
mysql> SELECT json_set('{"arr": [10, 20]}', '$.arr[5]', 30);
       -> {"arr": [10, 20, 30]}
```

Example 7: Insert different data types (Boolean and JSON Null).
To insert a JSON `null` value, use `parse_json('null')`. Passing a raw SQL `NULL` will return `NULL` for the entire result.

```plaintext
mysql> SELECT json_set('{"a": 1}', '$.b', true, '$.c', parse_json('null'));
       -> {"a": 1, "b": true, "c": null}
```

## Usage notes

- The `json_set` function follows MySQL-compatible behavior.
- It operates as an **Upsert** (Update or Insert):
    - **INSERT:** If the path does not exist, the value is added to the document.
    - **UPDATE:** If the path already exists, the old value is replaced with the new value.
- If you specifically want to insert *only* (without updating existing values), use `json_insert`.
- If you specifically want to update *only* (without inserting new values), use `json_replace`.
- **Null Handling:** To insert a JSON null value, use parse_json('null'). Passing a raw SQL NULL as an argument will cause the function to return NULL.
- **Note:** Wildcard characters (e.g., `*` or `**`) and array slices (e.g., `[1:3]`) are not currently supported in `json_path` for modification. If a path contains these, the update for that path will be ignored to ensure safety.