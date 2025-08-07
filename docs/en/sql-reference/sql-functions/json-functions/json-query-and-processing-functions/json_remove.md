---
displayed_sidebar: docs
---

# json_remove

Removes data from a JSON document at one or more specified JSON paths and returns the modified JSON document.

:::tip
All of the JSON functions and operators are listed in the navigation and on the [overview page](../overview-of-json-functions-and-operators.md)
:::

## Syntax

```Haskell
json_remove(json_object_expr, json_path[, json_path] ...)
```

## Parameters

- `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

- `json_path`: one or more expressions that represent the paths to elements in the JSON object that should be removed. The value of each parameter is a string. For information about the JSON path syntax that is supported by StarRocks, see [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md).

## Return value

Returns a JSON document with the specified paths removed.

> - If a path does not exist in the JSON document, it is ignored.
> - If an invalid path is provided, it is ignored.
> - If all paths are invalid or non-existent, the original JSON document is returned unchanged.

## Examples

Example 1: Remove a single key from a JSON object.

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30]}', '$.a');

       -> {"b": [10, 20, 30]}
```

Example 2: Remove multiple keys from a JSON object.

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30], "c": "test"}', '$.a', '$.c');

       -> {"b": [10, 20, 30]}
```

Example 3: Remove array elements from a JSON object.

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30]}', '$.b[1]');

       -> {"a": 1, "b": [10, 30]}
```

Example 4: Remove nested object properties.

```plaintext
mysql> SELECT json_remove('{"a": {"x": 1, "y": 2}, "b": 3}', '$.a.x');

       -> {"a": {"y": 2}, "b": 3}
```

Example 5: Attempt to remove non-existent paths (ignored).

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": 2}', '$.c', '$.d');

       -> {"a": 1, "b": 2}
```

Example 6: Remove multiple paths including non-existent ones.

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": 2, "c": 3}', '$.a', '$.nonexistent', '$.c');

       -> {"b": 2}
```

## Usage notes

- The `json_remove` function follows MySQL-compatible behavior.
- Invalid JSON paths are silently ignored rather than causing errors.
- The function supports removing multiple paths in a single operation, which is more efficient than multiple separate operations.
- Currently, the function supports simple object key removal (e.g., `$.key`). Support for complex nested paths and array element removal may be limited in the current implementation.