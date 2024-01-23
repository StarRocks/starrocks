---
displayed_sidebar: "English"
---

# unnest_bitmap

## Description

unnest_bitmap is a table function that takes a bitmap and converts elements in that bitmap into multiple rows of a table.

StarRocks' [Lateral Join](../../../using_starrocks/Lateral_join.md) can be used in conjunction with the unnest_bitmap function to implement common column-to-row logic.

This function can be used to replace `unnest(bitmap_to_array(bitmap))`. It has better performance, takes up less memory resources, and is not limited by the length of the array.

This function is supported from v3.1.

## Syntax

```Haskell
unnest_bitmap(bitmap)
```

## Parameters

`bitmap`: the bitmap you want to convert.

## Return value

Returns multiple rows converted from the bitmap. The type of return value is BIGINT.

## Examples

`c2` is a bitmap column in table `t1`.

```SQL
-- Use the bitmap_to_string function to convert values in the c2 column into a string.
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

-- Use the unnest_bitmap function to expand the bitmap column into multiple rows.
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
