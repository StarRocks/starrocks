---
displayed_sidebar: "English"
---

# get_json_bool

## Description

This function analyzes and gets the boolean value from a specified path in json string.

json_path must start with `$` and use `.` as the path separator.

If the path includes `.`, it could be circled by `"` and `"`.

`[ ]` is used as the array subscripts which start from 0.

Content in the path should not contain `"` , `[` and `]`.

If the format of json_string or json_path is wrong, this function will return NULL.

## Syntax

```Haskell
BOOLEAN get_json_bool(VARCHAR json_str, VARCHAR json_path)
```

## Examples

1. Get the value whose key is "k1"

    ```Plain Text
   MySQL > SELECT get_json_bool('{"k1":true, "k2":"false"}', "$.k1");
   +----------------------------------------------------+
   | get_json_bool('{"k1":true, "k2":"false"}', '$.k1') |
   +----------------------------------------------------+
   |                                                  1 |
   +----------------------------------------------------+
    ```

2. Get the second element in the array whose key is "my.key"

    ```Plain Text
   SELECT get_json_bool('{"k1":"v1", "my.key":[true, false, 3]}', '$."my.key"[1]');
   +--------------------------------------------------------------------------+
   | get_json_bool('{"k1":"v1", "my.key":[true, false, 3]}', '$."my.key"[1]') |
   +--------------------------------------------------------------------------+
   |                                                                        0 |
   +--------------------------------------------------------------------------+
    ```

3. Get the first element in the array whose path is k1.key -> k2.

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
