---
displayed_sidebar: docs
description: "Returns whether no elements of an array match the given predicate."
---

# none_match



Returns whether no elements of an array match the given predicate.

- Returns `true` (1) if none of the elements matches the predicate (a special case is when the array is empty).

- Returns `false` (0) if one or more elements match the predicate.

- Returns NULL if the predicate returns NULL for one or more elements and `false` for all other elements.

This function is supported from v26.3 onwards.

## Syntax

```Haskell
none_match(lambda_function, arr1, arr2...)
```

Returns whether no elements of `arr1` match the given predicate in the lambda function.

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
- `none_match()` is the negation of [any_match()](any_match.md): `none_match(f, arr)` returns the same result as `NOT any_match(f, arr)`.
- To apply this function to MAP, rewrite `none_match((k,v)->k>v,map)` to `none_match(map_values(transform_values((k,v)->k>v, map)))`. For example, `select none_match(map_values(transform_values((k,v)->k>v, map{2:1})));` returns 0.

## Examples

Check whether no element in `x` is less than the elements in `y`.

```Plain
select none_match((x,y) -> x < y, [1,2,8], [4,5,6]);
+---------------------------------------------------+
| none_match((x, y) -> x < y, [1, 2, 8], [4, 5, 6]) |
+---------------------------------------------------+
|                                                 0 |
+---------------------------------------------------+

select none_match((x,y) -> x < y, [11,12,8], [4,5,6]);
+-----------------------------------------------------+
| none_match((x, y) -> x < y, [11, 12, 8], [4, 5, 6]) |
+-----------------------------------------------------+
|                                                   1 |
+-----------------------------------------------------+

select none_match((x,y) -> x < y, [11,12,null], [4,5,6]);
+--------------------------------------------------------+
| none_match((x, y) -> x < y, [11, 12, NULL], [4, 5, 6]) |
+--------------------------------------------------------+
|                                                   NULL |
+--------------------------------------------------------+

select none_match((x,y) -> x < y, [], []);
+-------------------------------------+
| none_match((x, y) -> x < y, [], []) |
+-------------------------------------+
|                                   1 |
+-------------------------------------+

select none_match((x,y) -> x < y, null, [4,5,6]);
+----------------------------------------------+
| none_match((x, y) -> x < y, NULL, [4, 5, 6]) |
+----------------------------------------------+
|                                         NULL |
+----------------------------------------------+
```
