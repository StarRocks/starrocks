# intersect_count

## Description

A function to find the intersection size of a bitmap, without requiring the data distribution to be orthogonal. The first parameter is the Bitmap column, the second parameter is the dimension column used for filtering, and the third parameter is a variable-length parameter, meaning that different values of the filtering dimension column are taken.

## Syntax

```Haskell
BITMAP INTERSECT_COUNT(bitmap_column, column_to_filter, filter_values)
```

## Parameters

`bitmap_column`: the bitmap column to be computed..
`column_to_filter`: the name of the column to be intersected.
`filter_values`: the different values of the filtered dimension columns.

## Return value

Returns a value of the bigint type.

## Example

```SQL
mysql>  select intersect_count(user_id, dt, '2020-10-01', '2020-10-02'), intersect_count(user_id, dt, '2020-10-01') from tbl where dt in ('2020-10-01', '2020-10-02');
+--------------------------------------------------------------+------------------------------------------------+
| intersect_count(`user_id`, `dt`, '2020-10-01', '2020-10-02') | intersect_count(`user_id`, `dt`, '2020-10-01') |
+--------------------------------------------------------------+------------------------------------------------+
|                                                            3 |                                              7 |
+--------------------------------------------------------------+------------------------------------------------+
```