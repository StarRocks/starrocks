---
displayed_sidebar: docs
---

# get_json_int

## Description

This function analyzes and gets the integer value from a specified path in json string.

json_path must start with `$` and use `.` as the path separator.

If the path includes `.`, it could be circled by `"` and `"`.

`[ ]` is used as the array subscripts which start from 0.

Content in the path should not contain `"` , `[` and `]`.

If the format of json_string or json_path is wrong, this function will return NULL.

## Syntax

```Haskell
BIGINT get_json_int(VARCHAR json_str, VARCHAR json_path)
```

## Examples

1. Get the value whose key is "k1"

    ```Plain Text
    MySQL > SELECT get_json_int('{"k1":1, "k2":"2"}', "$.k1");
    +--------------------------------------------+
    | get_json_int('{"k1":1, "k2":"2"}', '$.k1') |
    +--------------------------------------------+
    |                                          1 |
    +--------------------------------------------+
    ```

2. Get the second element in the array whose key is "my.key"

    ```Plain Text
    MySQL > SELECT get_json_int('{"k1":"v1", "my.key":[1, 2, 3]}', '$."my.key"[1]');
    +------------------------------------------------------------------+
    | get_json_int('{"k1":"v1", "my.key":[1, 2, 3]}', '$."my.key"[1]') |
    +------------------------------------------------------------------+
    |                                                                2 |
    +------------------------------------------------------------------+
    ```

3. Get the first element in the array whose path is k1.key -> k2.

    ```Plain Text
    MySQL > SELECT get_json_int('{"k1.key":{"k2":[1, 2]}}', '$."k1.key".k2[0]');
    +--------------------------------------------------------------+
    | get_json_int('{"k1.key":{"k2":[1, 2]}}', '$."k1.key".k2[0]') |
    +--------------------------------------------------------------+
    |                                                            1 |
    +--------------------------------------------------------------+
    ```

## keyword

GET_JSON_INT,GET,JSON,INT
