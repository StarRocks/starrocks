---
displayed_sidebar: docs
---

# json_string

Converting JSON object to JSON string

:::tip
All of the JSON functions and operators are listed in the navigation and on the [overview page](../overview-of-json-functions-and-operators.md)

Accelerate your queries with [generated columns](../../../sql-statements/generated_columns.md)
:::

## Syntax

```SQL
json_string(json_object_expr)
```

## Parameters

- `json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

## Return value

Returns a VARCHAR value.

## Examples

Example 1: Converting the JSON object to a JSON string

```Plain
select json_string('{"Name": "Alice"}');
+----------------------------------+
| json_string('{"Name": "Alice"}') |
+----------------------------------+
| {"Name": "Alice"}                |
+----------------------------------+
```

Example 1: Convert the result of PARSE_JSON to a JSON string

```Plain
select json_string(parse_json('{"Name": "Alice"}'));
+----------------------------------+
| json_string('{"Name": "Alice"}') |
+----------------------------------+
| {"Name": "Alice"}                |
+----------------------------------+
```
