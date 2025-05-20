---
displayed_sidebar: docs
---

# json_array

Converts each element of an SQL array to a JSON value and returns a JSON array that consists of the JSON values.

:::tip
All of the JSON functions and operators are listed in the navigation and on the [overview page](../overview-of-json-functions-and-operators.md)

Accelerate your queries with [generated columns](../../../sql-statements/generated_columns.md)
:::

## Syntax

```Haskell
json_array(value, ...)
```

## Parameters

`value`: an element in the SQL array. Only `NULL` values and the following data types are supported: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, and BOOLEAN.

## Return value

Returns a JSON array.

## Examples

Example 1: Construct a JSON array that consists of values of different data types.

```plaintext
mysql> SELECT json_array(1, true, 'starrocks', 1.1);

       -> [1, true, "starrocks", 1.1]
```

Example 2: Construct an empty JSON array.

```plaintext
mysql> SELECT json_array();

       -> []
```