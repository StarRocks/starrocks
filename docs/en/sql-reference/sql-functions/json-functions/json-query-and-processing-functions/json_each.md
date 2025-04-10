---
displayed_sidebar: docs
---

# json_each

## Description

Expands the outermost elements of a JSON object into a set of key-value pairs held in two columns and returns a table that consists of one row for each element.

## Syntax

```Haskell
json_each(json_object_expr)
```

## Parameters

`json_object_expr`: the expression that represents the JSON object. The object can be a JSON column, or a JSON object that is produced by a JSON constructor function such as PARSE_JSON.

## Return value

Returns two columns: one named key and one named value. The key column stores VARCHAR values, and the value column stores JSON values.

## Usage notes

The json_each function is a table function that returns a table. The returned table is a result set that consists of multiple rows. Therefore, a lateral join must be used in the FROM clause to join the returned table to the original table. The lateral join is mandatory, but the LATERAL keyword is optional. The json_each function cannot be used in the SELECT clause.

## Examples

```plaintext
-- A table named tj is used as an example. In the tj table, the j column is a JSON object.
mysql> SELECT * FROM tj;
+------+------------------+
| id   | j                |
+------+------------------+
|    1 | {"a": 1, "b": 2} |
|    3 | {"a": 3}         |
+------+------------------+

-- Expand the j column of the tj table into two columns by key and value to obtain a result set that consists of multiple rows. In this example, the LATERAL keyword is used to join the result set to the tj table.

mysql> SELECT * FROM tj, LATERAL json_each(j);
+------+------------------+------+-------+
| id   | j                | key  | value |
+------+------------------+------+-------+
|    1 | {"a": 1, "b": 2} | a    | 1     |
|    1 | {"a": 1, "b": 2} | b    | 2     |
|    3 | {"a": 3}         | a    | 3     |
+------+------------------+------+-------+
```
