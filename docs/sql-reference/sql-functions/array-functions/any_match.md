# any_match

## Description

Returns whether any elements of an array match the given predicate.
Returns true if one or more elements match the predicate; false if none of the elements matches (a special case is when the array is empty); NULL if the predicate function returns NULL for one or more elements and false for all other elements.

## Syntax

```Haskell
any_match(lambda_function, arr1, arr2...)
```
Returns whether any elements of `arr1` match the given predicate in the lambda function.


## Parameters

`arr1`: the array to match.

`arrN`: optional arrays used in the lambda function.

`lambda_function`: the lambda function used to match values.

## Usage notes

- The lambda function follows the usage notes in [array_map()](array_map.md), it returns array<bool>.
- If the input array is null or the lambda function results null, null is returned.
- if `arr1` is empty, return false.

## Examples

```Plain
-- check whether any element in x that is less than the elements in y.

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

mysql> select any_match((x,y) -> x < y, null, [4,5,6]);
+---------------------------------------------+
| any_match((x, y) -> x < y, NULL, [4, 5, 6]) |
+---------------------------------------------+
|                                        NULL |
+---------------------------------------------+
```
