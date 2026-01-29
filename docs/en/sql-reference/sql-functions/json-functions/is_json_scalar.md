---
displayed_sidebar: docs
---

# is_json_scalar

Returns whether a JSON value is a scalar (not an object or array).

## Aliases
None

## Syntax

```Haskell
BOOLEAN is_json_scalar(JSON)
```

### Parameters

`json` : A value of JSON type. The function examines the parsed JSON value and determines if it is a scalar. Valid JSON scalar types include number, string, boolean, and JSON null. If the input is SQL NULL (or the JSON column value is NULL), the function returns SQL NULL.

## Return value

Returns a BOOLEAN:
- TRUE (1) if the JSON value is a scalar (number, string, boolean, or JSON null).
- FALSE (0) if the JSON value is a non-scalar (an object or an array).
- NULL if the input is SQL NULL.

## Usage notes

- The function operates on values of JSON type. Pass JSON expressions or CAST string literals to JSON: e.g., CAST('{"a": 1}' AS JSON).
- JSON null (the literal JSON value null) is considered a scalar by this function because it is not an object or array.
- SQL NULL (absence of a value) is distinct from JSON null; SQL NULL yields a NULL result.
- The function only inspects the top-level JSON value: object and array are non-scalar regardless of their contents.
- This function is implemented as JsonFunctions::is_json_scalar, which checks that the underlying VPack slice is not an object and not an array.

## Examples

```Plain
mysql> SELECT is_json_scalar(CAST('{"a": 1}' AS JSON));
+----------------------------------------------+
| is_json_scalar(CAST('{"a": 1}' AS JSON))     |
+----------------------------------------------+
| 0                                            |
+----------------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('[1, 2, 3]' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('[1, 2, 3]' AS JSON)) |
+-----------------------------------------+
| 0                                       |
+-----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('"hello"' AS JSON));
+-------------------------------------------+
| is_json_scalar(CAST('"hello"' AS JSON))   |
+-------------------------------------------+
| 1                                         |
+-------------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('123' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('123' AS JSON))    |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('true' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('true' AS JSON))    |
+-----------------------------------------+
| 1                                       |
+-----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('null' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('null' AS JSON))   |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST(NULL AS JSON));
+------------------------------+
| is_json_scalar(CAST(NULL AS JSON)) |
+------------------------------+
| NULL                         |
+------------------------------+
```

## keyword
IS_JSON_SCALAR, None