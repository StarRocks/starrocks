# all_match

## Description

Returns whether all elements of an array match the given predicate. 
Returns true if all the elements match the predicate (a special case is when the array is empty); false if one or more elements don’t match; NULL if the predicate function returns NULL for one or more elements and true for all other elements.


## Syntax

```Haskell
all_match(lambda_function, arr1, arr2...)
```

Returns whether all elements of `arr1` match the given predicate in the lambda function.

## Parameters

`arr1`: the array to match.

`arrN`: optional arrays used in the lambda function.

`lambda_function`: the lambda function used to match values.

## Usage notes

- The lambda function follows the usage notes in [array_map()](array_map.md), it returns array<bool>.
- If the input array is null or the lambda function results null, null is returned.
- if `arr1` is empty, return true.

## Examples

```Plain
-- check whether all elements in x that are less than the elements in y.

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

mysql> select all_match((x,y) -> x < y, null, [4,5,6]);
+---------------------------------------------+
| all_match((x, y) -> x < y, NULL, [4, 5, 6]) |
+---------------------------------------------+
|                                        NULL |
+---------------------------------------------+

```