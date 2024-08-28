---
displayed_sidebar: docs
---

# get_json_bool

## Description

Parses and gets the boolean value from a specified JSON path in a JSON string.

If the format of `json_str` or `json_path` is invalid, or if no matching content can be found, this function will return NULL.

This function is supported from v3.3.

## Syntax

```Haskell
BOOLEAN get_json_bool(VARCHAR json_str, VARCHAR json_path)
```

## Parameters

- `json_str`: the JSON string. The supported data type is VARCHAR.
- `json_path`: the JSON path. The supported data type is VARCHAR.
   
  - `json_path` must start with `$` and use `.` as the path separator. If the path includes `.`, it can be enclosed in a pair of `"`.
  - `[ ]` is used as the array subscripts which starts from 0.

## Examples

1. Get the value whose key is "k1". The value is `true` and `1` is returned.

    ```Plain Text
   MySQL > SELECT get_json_bool('{"k1":true, "k2":"false"}', "$.k1");
   +----------------------------------------------------+
   | get_json_bool('{"k1":true, "k2":"false"}', '$.k1') |
   +----------------------------------------------------+
   |                                                  1 |
   +----------------------------------------------------+
    ```

2. Get the second element in the array whose key is "my.key". The second element is `false` and `0` is returned.

    ```Plain Text
   SELECT get_json_bool('{"k1":"v1", "my.key":[true, false, 3]}', '$."my.key"[1]');
   +--------------------------------------------------------------------------+
   | get_json_bool('{"k1":"v1", "my.key":[true, false, 3]}', '$."my.key"[1]') |
   +--------------------------------------------------------------------------+
   |                                                                        0 |
   +--------------------------------------------------------------------------+
    ```

3. Get the first element in the array whose path is `k1.key -> k2`. The first element is `false` and `0` is returned.

    ```Plain Text
   MYSQL > SELECT get_json_bool('{"k1.key":{"k2":[false, true]}}', '$."k1.key".k2[0]');
   +----------------------------------------------------------------------+
   | get_json_bool('{"k1.key":{"k2":[false, true]}}', '$."k1.key".k2[0]') |
   +----------------------------------------------------------------------+
   |                                                                    0 |
   +----------------------------------------------------------------------+
    ```

## keyword

GET_JSON_BOOL,GET,JSON,BOOL
