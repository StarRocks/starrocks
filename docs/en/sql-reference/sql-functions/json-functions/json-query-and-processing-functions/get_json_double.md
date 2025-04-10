---
displayed_sidebar: docs
---

# get_json_double

## Description

This function analyzes and gets the floating point value from a specified path in json string. json_path must start with `$` and use `.` as the path separator.

If the path includes `.`, it could be circled by `"` and `"`.

`[ ]` is used as the array subscripts which start from 0.

Content in the path should not contain `"` , `[` and `]`.

If the format of json_string or json_path is wrong, this function will return NULL.

## Syntax

```Haskell
DOUBLE get_json_double(VARCHAR json_str, VARCHAR json_path)
```

## Examples

1. Get the value whose key is "k1"

    ```Plain Text
    MySQL > SELECT get_json_double('{"k1":1.3, "k2":"2"}', "$.k1");
    +-------------------------------------------------+
    | get_json_double('{"k1":1.3, "k2":"2"}', '$.k1') |
    +-------------------------------------------------+
    |                                             1.3 |
    +-------------------------------------------------+
    ```

2. Get the second element in the array whose key is "my.key"

    ```Plain Text
    MySQL > SELECT get_json_double('{"k1":"v1", "my.key":[1.1, 2.2, 3.3]}', '$."my.key"[1]');
    +---------------------------------------------------------------------------+
    | get_json_double('{"k1":"v1", "my.key":[1.1, 2.2, 3.3]}', '$."my.key"[1]') |
    +---------------------------------------------------------------------------+
    |                                                                       2.2 |
    +---------------------------------------------------------------------------+
    ```

3. Get the first element in the array whose path is k1.key -> k2.

    ```Plain Text
    MySQL > SELECT get_json_double('{"k1.key":{"k2":[1.1, 2.2]}}', '$."k1.key".k2[0]');
    +---------------------------------------------------------------------+
    | get_json_double('{"k1.key":{"k2":[1.1, 2.2]}}', '$."k1.key".k2[0]') |
    +---------------------------------------------------------------------+
    |                                                                 1.1 |
    +---------------------------------------------------------------------+
    ```

## keyword

GET_JSON_DOUBLE,GET,JSON,DOUBLE
