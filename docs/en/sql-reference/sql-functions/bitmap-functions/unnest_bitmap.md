---
displayed_sidebar: "English"
---

# unnest_bitmap

## Description

UNNEST_BITMAP is a table function that takes an bitmap and converts elements in that bitmap into multiple rows of a table. 

StarRocks' lateral join can be used in conjunction with the unnest_bitmap function to implement common column-to-row logic.

This function can be used to replace unnest(bitmap_to_array(bitmap)), which has better performance, takes up less memory resources, and is not limited by the length of the array.

This function is supported from v3.1.

## Syntax

```Haskell
unnest_bitmap(bitmap)
```

## Parameters

`bitmap`: the bitmap you want to convert. 

## Return value

Returns the multiple rows converted from the bitmap. The type of return value should be bigint.

## Examples

`c2` is bitmap column.

```SQL
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

-- Use the unnest_bitmap function to expand bitmap columns.
mysql> select c1, unnest_bitmap from t1, unnest_bitmap(c2);
+------+---------------+
| c1   | unnest_bitmap |
+------+---------------+
|    1 |             1 |
|    1 |             2 |
|    1 |             3 |
|    1 |             4 |
|    1 |             5 |
|    1 |             6 |
|    1 |             7 |
|    1 |             8 |
|    1 |             9 |
|    1 |            10 |
+------+---------------+
```
