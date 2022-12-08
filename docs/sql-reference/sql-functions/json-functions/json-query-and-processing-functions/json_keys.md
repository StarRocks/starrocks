# json_keys

## Description

Returns the top-level keys from a JSON object as a JSON array, or, if a `path` is specified, the top-level keys from the path.

## Syntax

```Haskell
json_keys(json_doc[, path])
```

## Parameters

`json_doc`: required. The JSON document for which to return the keys. It must be a JSON object.

`path`: optional. The path generally starts with `$` and uses `.` as the path separator. `[]` is used as the array subscript, which starts from 0.

## Return value

Returns a JSON array.

An empty array is returned if the JSON object is empty. 

`NULL` is returned if the JSON document is not a JSON object or the path does not identify a value in the document.

If the JSON document is an array nested with a JSON object, you can use the `path` parameter to obtain keys from that object.

## Examples

Example 1: Return an empty array because the input JSON object is empty.

```Plain
select json_keys('{}');
+-----------------+
| json_keys('{}') |
+-----------------+
| []              |
+-----------------+
```

Example 2: Return the keys of a JSON object.

```Plain
select json_keys('{"a": 1, "b": 2, "c": 3}');
+----------------+
| json_keys('1') |
+----------------+
|["a", "b", "c"] |
+----------------+
```

Example 3: Return the keys of a JSON object that matches the specified path.

```Plain
select json_keys('{"a": 1, "b": 2, "c": {"d": 3, "e": 4, "f": 5}}', '$.c');
+---------------------------------------------------------------------+
| json_keys('{"a": 1, "b": 2, "c": {"d": 3, "e": 4, "f": 5}}', '$.c') |
+---------------------------------------------------------------------+
| ["d", "e", "f"]                                                     |
+---------------------------------------------------------------------+
```

Example 4: The path does not exist.

```Plain
select json_keys('{"a": 1, "b": 2, "c": {"d": 3, "e": 4, "f": 5}}', '$.e');
+---------------------------------------------------------------------+
| json_keys('{"a": 1, "b": 2, "c": {"d": 3, "e": 4, "f": 5}}', '$.e') |
+---------------------------------------------------------------------+
| NULL                                                                |
+---------------------------------------------------------------------+
```

Example 5: The JSON document is not a JSON object.

```Plain
select json_keys('[1, 2, {"a": 1, "b": 2}]');
+---------------------------------------+
| json_keys('[1, 2, {"a": 1, "b": 2}]') |
+---------------------------------------+
| NULL                                  |
+---------------------------------------+
```

Example 6: The JSON document is an array nested with a JSON object. A path is specified to obtain keys from that object.

```Plain
select json_keys('[0, 1, {"a": 1, "b": 2}]', '$[2]');
+-----------------------------------------------+
| json_keys('[0, 1, {"a": 1, "b": 2}]', '$[2]') |
+-----------------------------------------------+
| ["a", "b"]                                    |
+-----------------------------------------------+
```
