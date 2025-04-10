---
displayed_sidebar: docs
---

# all_match

## Description

Returns whether all elements of an array match the given predicate.

- Returns `true` (1) if all the elements match the predicate (a special case is when the array is empty).

- Returns `false` (0) if one or more elements do not match.

- Returns NULL if the predicate returns NULL for one or more elements and `true` for all other elements.

This function is supported from v3.0.6 onwards.

## Syntax

```Haskell
all_match(lambda_function, arr1, arr2...)
```

Returns whether all elements of `arr1` match the given predicate in the lambda function.

## Parameters

- `arr1`: the array to match.

- `arrN`: optional arrays used in the lambda function.

- `lambda_function`: the lambda function used to match values.

## Return value

Returns a BOOLEAN value.

## Usage notes

- The lambda function follows the usage notes in [array_map()](array_map.md).
- If the input array is null or the lambda function results in null, null is returned.
- If `arr1` is empty, `true` is returned.
- To apply this function to MAP, rewrite `all_match((k,v)->k>v,map)` to `all_match(map_values(transform_values((k,v)->k>v, map)))`. For example, `select all_match(map_values(transform_values((k,v)->k>v, map{2:1})));` returns 1.

## Examples

Check whether all elements in `x` are less than the elements in `y`.

```Plain
select all_match((x,y) -> x < y, [1,2,-8], [4,5,6]);
+---------------------------------------------------+
| all_match((x, y) -> x < y, [1, 2, -8], [4, 5, 6]) |
+---------------------------------------------------+
|                                                 1 |
+---------------------------------------------------+

select all_match((x,y) -> x < y, [1,2,null], [4,5,6]);
+-----------------------------------------------------+
| all_match((x, y) -> x < y, [1, 2, NULL], [4, 5, 6]) |
+-----------------------------------------------------+
|                                                NULL |
+-----------------------------------------------------+

select all_match((x,y) -> x < y, [1,2,8], [4,5,6]);
+--------------------------------------------------+
| all_match((x, y) -> x < y, [1, 2, 8], [4, 5, 6]) |
+--------------------------------------------------+
|                                                0 |
+--------------------------------------------------+

select all_match((x,y) -> x < y, [], []);
+------------------------------------+
| all_match((x, y) -> x < y, [], []) |
+------------------------------------+
|                                  1 |
+------------------------------------+

select all_match((x,y) -> x < y, null, [4,5,6]);
+---------------------------------------------+
| all_match((x, y) -> x < y, NULL, [4, 5, 6]) |
+---------------------------------------------+
|                                        NULL |
+---------------------------------------------+
```
