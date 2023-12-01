---
displayed_sidebar: "English"
---

# any_match

## Description

Returns whether any elements of an array match the given predicate.

- Returns `true` (1) if one or more elements match the predicate.

- Returns `false` (0) if none of the elements matches (a special case is when the array is empty).

- Returns NULL if the predicate returns NULL for one or more elements and `false` for all other elements.

This function is supported from v3.0.6 onwards.

## Syntax

```Haskell
any_match(lambda_function, arr1, arr2...)
```

Returns whether any elements of `arr1` match the given predicate in the lambda function.

## Parameters

- `arr1`: the array to match.

- `arrN`: optional arrays used in the lambda function.

- `lambda_function`: the lambda function used to match values.

## Return value

Returns a BOOLEAN value.

## Usage notes

- The lambda function follows the usage notes in [array_map()](array_map.md).
- If the input array is null or the lambda function results in null, null is returned.
- If `arr1` is empty, `false` is returned.
- To apply this function to MAP, rewrite `any_match((k,v)->k>v,map)` to `any_match(map_values(transform_values((k,v)->k>v, map)))`. For example, `select any_match(map_values(transform_values((k,v)->k>v, map{2:1})));` returns 1.

## Examples

Check whether any element in `x` is less than the elements in `y`.

```Plain
select any_match((x,y) -> x < y, [1,2,8], [4,5,6]);
+--------------------------------------------------+
| any_match((x, y) -> x < y, [1, 2, 8], [4, 5, 6]) |
+--------------------------------------------------+
|                                                1 |
+--------------------------------------------------+

select any_match((x,y) -> x < y, [11,12,8], [4,5,6]);
+----------------------------------------------------+
| any_match((x, y) -> x < y, [11, 12, 8], [4, 5, 6]) |
+----------------------------------------------------+
|                                                  0 |
+----------------------------------------------------+

select any_match((x,y) -> x < y, [11,12,null], [4,5,6]);
+-------------------------------------------------------+
| any_match((x, y) -> x < y, [11, 12, NULL], [4, 5, 6]) |
+-------------------------------------------------------+
|                                                  NULL |
+-------------------------------------------------------+

select any_match((x,y) -> x < y, [], []);
+------------------------------------+
| any_match((x, y) -> x < y, [], []) |
+------------------------------------+
|                                  0 |
+------------------------------------+

select any_match((x,y) -> x < y, null, [4,5,6]);
+---------------------------------------------+
| any_match((x, y) -> x < y, NULL, [4, 5, 6]) |
+---------------------------------------------+
|                                        NULL |
+---------------------------------------------+
```
