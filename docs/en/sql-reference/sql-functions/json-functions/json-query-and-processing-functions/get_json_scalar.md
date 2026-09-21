---
displayed_sidebar: docs
description: "Parses and gets the scalar value from a specified JSON path in a JSON string, returning NULL if the matched value is an object or array."
---

# get_json_scalar

Parses and gets the scalar value from a specified JSON path in a JSON string, and returns it as a VARCHAR.

:::tip
All of the JSON functions and operators are listed in the navigation and on the [overview page](../overview-of-json-functions-and-operators.md)

Accelerate your queries with [generated columns](../../../sql-statements/generated_columns.md)
:::

If the format of `json_str` or `json_path` is invalid, or if no matching content can be found, this function will return NULL. If the value located by `json_path` is a JSON object or a JSON array, this function also returns NULL, because it only returns scalar values (numbers, strings, booleans, and JSON null).

## Syntax

```Haskell
VARCHAR get_json_scalar(VARCHAR json_str, VARCHAR json_path)
```

## Parameters

- `json_str`: the JSON string. The supported data type is VARCHAR.
- `json_path`: the JSON path. The supported data type is VARCHAR.
   
  - `json_path` must start with `$` and use `.` as the path separator. If the path includes `.`, it can be enclosed in a pair of `"`.
  - `[ ]` is used as the array subscripts which starts from 0.

## Examples

1. Get the value whose key is "k1". The value is the number `1`, and `1` is returned.

    ```Plain Text
   MySQL > SELECT get_json_scalar('{"k1":1, "k2":"v2"}', "$.k1");
   +----------------------------------------------------+
   | get_json_scalar('{"k1":1, "k2":"v2"}', '$.k1')     |
   +----------------------------------------------------+
   | 1                                                  |
   +----------------------------------------------------+
    ```

2. Get the second element in the array whose key is "my.key". The second element is `false` and `false` is returned.

    ```Plain Text
    MySQL > SELECT get_json_scalar('{"k1":"v1", "my.key":[true, false, 3]}', '$."my.key"[1]');
    +-----------------------------------------------------------------------------+
    | get_json_scalar('{"k1":"v1", "my.key":[true, false, 3]}', '$."my.key"[1]')  |
    +-----------------------------------------------------------------------------+
    | false                                                                       |
    +-----------------------------------------------------------------------------+
    ```

3. Get the first element in the array whose path is `k1.key -> k2`. The first element is `false` and `false` is returned.

    ```Plain Text
    MySQL > SELECT get_json_scalar('{"k1.key":{"k2":[false, true]}}', '$."k1.key".k2[0]');
    +-------------------------------------------------------------------------+
    | get_json_scalar('{"k1.key":{"k2":[false, true]}}', '$."k1.key".k2[0]')  |
    +-------------------------------------------------------------------------+
    | false                                                                   |
    +-------------------------------------------------------------------------+
    ```

4. Get the value whose key is "k1", which is a JSON object rather than a scalar value. NULL is returned.

    ```Plain Text
    MySQL > SELECT get_json_scalar('{"k1":{"k11":"v11"}}', '$.k1');
    +----------------------------------------------------+
    | get_json_scalar('{"k1":{"k11":"v11"}}', '$.k1')    |
    +----------------------------------------------------+
    | NULL                                               |
    +----------------------------------------------------+
    ```

## keyword

GET_JSON_SCALAR,GET,JSON,SCALAR
