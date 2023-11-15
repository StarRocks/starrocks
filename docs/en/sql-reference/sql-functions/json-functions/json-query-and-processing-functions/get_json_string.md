# get_json_string

## Description

This function analyzes and gets the strings from a specified path in json string.

json_path must start with `$` and use `.` as the path separator.

If the path includes `.`, it could be circled by `"` and `"`.

`[ ]` is used as the array subscripts which start from 0.

Content in the path should not contain `"` , `[` and `]`.

If the format of json_string or json_path is wrong, this function will return NULL.

## Syntax

```Haskell
VARCHAR get_json_string(VARCHAR json_str, VARCHAR json_path)
```

## Examples

1. Get the value whose key is "k1"

    ```Plain Text
    MySQL > SELECT get_json_string('{"k1":"v1", "k2":"v2"}', "$.k1");
    +---------------------------------------------------+
    | get_json_string('{"k1":"v1", "k2":"v2"}', '$.k1') |
    +---------------------------------------------------+
    | v1                                                |
    +---------------------------------------------------+
    ```

2. Get the second element in the array whose key is "my.key"

    ```Plain Text
    MySQL > SELECT get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]');
    +------------------------------------------------------------------------------+
    | get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]') |
    +------------------------------------------------------------------------------+
    | e2                                                                           |
    +------------------------------------------------------------------------------+
    ```

3. Get the first element in the array whose path is k1.key -> k2.

    ```Plain Text
    MySQL > SELECT get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]');
    +-----------------------------------------------------------------------+
    | get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]') |
    +-----------------------------------------------------------------------+
    | v1                                                                    |
    +-----------------------------------------------------------------------+
    ```

4. Get all values whose key is "k1" from the array.

    ```Plain Text
    MySQL > SELECT get_json_string('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}]', '$.k1');
    +---------------------------------------------------------------------------------+
    | get_json_string('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}]', '$.k1') |
    +---------------------------------------------------------------------------------+
    | ["v1","v3","v4"]                                                                |
    +---------------------------------------------------------------------------------+
    ```

## keyword

GET_JSON_STRING,GET,JSON,STRING
