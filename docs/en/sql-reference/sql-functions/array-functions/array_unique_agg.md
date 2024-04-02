---
displayed_sidebar: "English"
---

# array_unique_agg

## Description

Aggregates distinct values (including `NULL`) in an ARRAY column into an array (from multiple rows to one row).

This function is supported from v3.1.8.

## Syntax

```Haskell
ARRAY_UNIQUE_AGG(col)
```

## Parameters

- `col`: the column whose values you want to aggregate. Supported data type is ARRAY.

## Return value

Returns a value of the ARRAY type.

## Usage notes

- The order of the elements in an array is random.
- The data type of the elements in the returned array is the same as the data type of the elements in the input column.
- Returns `NULL` if there is no matched value.

## Examples

Take the following data table as an example:

```plaintext
mysql > select * from array_unique_agg_example;
+------+--------------+
| a    | b            |
+------+--------------+
|    2 | [1,null,2,4] |
|    2 | [1,null,3]   |
|    1 | [1,1,2,3]    |
|    1 | [2,3,4]      |
+------+--------------+
```

Example 1: Group the values in column `a` and aggregate distinct values in column `b` into an array.

```plaintext
mysql > select a, array_unique_agg(b) from array_unique_agg_example group by a;
+------+---------------------+
| a    | array_unique_agg(b) |
+------+---------------------+
|    1 | [4,1,2,3]           |
|    2 | [4,1,2,3,null]      |
+------+---------------------+
```

Example 2: Aggregate values in column `b` using the WHERE clause. If no data meets the filter condition, a `NULL` value is returned.

```plaintext
mysql > select array_unique_agg(b) from array_unique_agg_example where a < 0;
+---------------------+
| array_unique_agg(b) |
+---------------------+
| NULL                |
+---------------------+
```

## Keywords

ARRAY_UNIQUE_AGG, ARRAY
