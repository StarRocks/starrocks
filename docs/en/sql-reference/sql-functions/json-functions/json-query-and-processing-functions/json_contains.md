---
displayed_sidebar: docs
---

# json_contains

Checks whether a JSON document contains a specific value or subdocument. If the target JSON document contains the candidate JSON value, the JSON_CONTAINS function returns `1`. Otherwise, the JSON_CONTAINS function returns `0`.

:::tip
All of the JSON functions and operators are listed in the navigation and on the [overview page](../overview-of-json-functions-and-operators.md)

:::

## Syntax

```Haskell
json_contains(json_target, json_candidate)
```

## Parameters

- `json_target`: the expression that represents the target JSON document. The document can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

- `json_candidate`: the expression that represents the candidate JSON value or subdocument to search for within the target. The value can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

## Return value

Returns a BOOLEAN value.

## Usage notes

- For scalar values (strings, numbers, booleans, null), the function returns true if the values are equal.
- For JSON objects, the function returns true if the target object contains all key-value pairs from the candidate object.
- For JSON arrays, the function returns true if the target array contains all elements from the candidate array, or if the candidate is a single value contained in the target array.
- The function performs deep containment checking for nested structures.

## Examples

Example 1: Check if a JSON object contains a specific key-value pair.

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"a": 1}'));

       -> 1
```

Example 2: Check if a JSON object contains a key-value pair that doesn't exist.

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"c": 3}'));

       -> 0
```

Example 3: Check if a JSON array contains specific elements.

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('[2, 3]'));

       -> 1
```

Example 4: Check if a JSON array contains a single scalar value.

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('2'));

       -> 1
```

Example 5: Check if a JSON array contains elements that don't exist.

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('[5, 6]'));

       -> 0
```

Example 6: Check containment with nested JSON structures.

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"users": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]}'), 
                           PARSE_JSON('{"users": [{"id": 1}]}'));

       -> 0
```

Note: In the last example, the result is 0 because array containment requires exact element matching, not partial object matching within arrays.