---
displayed_sidebar: docs
---

# json_pretty

Formats a JSON document into an easy-to-read, indented string format. This function is useful for debugging or displaying JSON data in a human-readable structure.

:::tip
All of the JSON functions and operators are listed in the navigation and on the overview page.
:::

## Syntax

```SQL
json_pretty(json_object_expr)
```

## Parameters
- `json_object_expr`: The expression that represents the JSON object. The object can be a JSON column, a string containing valid JSON, or a JSON object produced by a JSON constructor function such as PARSE_JSON.

## Return value
Returns the formatted JSON document as a string.

> - Returns NULL if the argument is NULL.
> - The returned string includes newlines and spaces for indentation.
> - Object keys are sorted alphabetically in the output.

## Examples

Example 1: Format a simple JSON object.

```Plaintext
mysql> SELECT json_pretty('{"b": 2, "a": 1}');
       -> {
            "a": 1,
            "b": 2
          }
```

Example 2: Format a JSON array.

```Plaintext
mysql> SELECT json_pretty('[1, 2, 3]');
       -> [
            1,
            2,
            3
          ]
```

Example 3: Format a nested JSON structure.

```Plaintext
mysql> SELECT json_pretty('{"level1": {"level2": {"level3": "value"}}}');
       -> {
            "level1": {
              "level2": {
                "level3": "value"
              }
            }
          }
```

Example 4: Use with a table column containing JSON data.

```Plaintext

mysql> CREATE TABLE json_test (id INT, data JSON);
mysql> INSERT INTO json_test VALUES (1, parse_json('{"name": "Alice", "details": {"age": 25, "city": "NYC"}}'));
mysql> SELECT json_pretty(data) FROM json_test;
       -> {
            "details": {
              "age": 25,
              "city": "NYC"
            },
            "name": "Alice"
          }
```

## Usage notes
- **Indentation:** The function adds standard indentation (spaces) and newlines to make the JSON structure visual.
- **Key Sorting:** JSON object keys are sorted alphabetically in the output string. This is standard behavior for the underlying JSON processing library (VelocyPack).
- **Null Handling:** If the input is SQL NULL, the function returns NULL.
- **Data Types:** It supports formatting of standard JSON types including Objects, Arrays, Strings, Numbers, Booleans, and Nulls.