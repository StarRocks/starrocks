---
displayed_sidebar: docs
---

# get_json_string,get_json_object

## Description

Analyzes and gets strings from the specified path (`json_path`) in a JSON string. If the format of `json_string` or `json_path` is wrong or if no matching value is found, this function will return NULL.

The alias is get_json_object.

## Syntax

```Haskell
VARCHAR get_json_string(VARCHAR json_str, VARCHAR json_path)
```

## Parameters

- `json_str`: the JSON string. Supported data type is VARCHAR.
- `json_path`: the JSON path. Supported data type is VARCHAR. `json_path` starts with `$` and uses `.` as the path separator. `[ ]` is used as the array subscripts which starts from 0. For example, `$."my.key"[1]` indicates to obtain the second value form element `my.key`.

## Return value

Returns a value of the VARCHAR type. If no matching object is found, NULL is returned.

## Examples

Example 1: Get the value whose key is `k1`.

```Plain Text
MySQL > SELECT get_json_string('{"k1":"v1", "k2":"v2"}', "$.k1");
+---------------------------------------------------+
| get_json_string('{"k1":"v1", "k2":"v2"}', '$.k1') |
+---------------------------------------------------+
| v1                                                |
+---------------------------------------------------+
```

Example 2: Get the value whose key is `a` from the first element.

```Plain Text
MySQL > SELECT get_json_object('[{"a":"123", "b": "456"},{"a":"23", "b": "56"}]', '$[0].a');
+------------------------------------------------------------------------------+
| get_json_object('[{"a":"123", "b": "456"},{"a":"23", "b": "56"}]', '$[0].a') |
+------------------------------------------------------------------------------+
| 123                                                                          |
+------------------------------------------------------------------------------+
```

Example 3: Get the second element in the array whose key is `my.key`

```Plain Text
MySQL > SELECT get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]');
+------------------------------------------------------------------------------+
| get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]') |
+------------------------------------------------------------------------------+
| e2                                                                           |
+------------------------------------------------------------------------------+
```

Example 4: Get the first element in the array whose path is `k1.key -> k2`.

```Plain Text
MySQL > SELECT get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]');
+-----------------------------------------------------------------------+
| get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]') |
+-----------------------------------------------------------------------+
| v1                                                                    |
+-----------------------------------------------------------------------+
```

Example 5: Get all values whose key is `k1` from the array.

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
