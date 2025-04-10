---
displayed_sidebar: docs
---

# intersect_count

## Description

A function to find the intersection size (number of same elements) of two Bitmap values, without requiring the data distribution to be orthogonal. The first parameter is the Bitmap column, the second parameter is the dimension column used for filtering, and the third parameter is a variable-length parameter, meaning that different values of the filtering dimension column are taken.

If there is no intersection, 0 is returned.

## Syntax

```Haskell
BIGINT INTERSECT_COUNT(bitmap_column, column_to_filter, filter_values)
```

## Parameters

- `bitmap_column`: the bitmap column to be computed.
- `column_to_filter`: the name of the column to be intersected.
- `filter_values`: the different values of the filtered dimension columns.

## Return value

Returns a value of the BIGINT type.

## Example

Create an Aggregate table with a Bitmap column `user_id` and insert data.

```SQL
CREATE TABLE `tbl` (
  `dt` date NULL COMMENT "",
  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
  
) ENGINE=OLAP
AGGREGATE KEY(`dt`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`dt`);

INSERT INTO `tbl` VALUES
('2020-10-01', to_bitmap(2)),
('2020-10-01', to_bitmap(3)),
('2020-10-01', to_bitmap(5)),
('2020-10-02', to_bitmap(2)),
('2020-10-02', to_bitmap(3)),
('2020-10-02', to_bitmap(4)),
('2020-10-03', to_bitmap(1)),
('2020-10-03', to_bitmap(5)),
('2020-10-03', to_bitmap(6));

SELECT dt, bitmap_to_string(user_id) from tbl;
+------------+---------------------------+
| dt         | bitmap_to_string(user_id) |
+------------+---------------------------+
| 2020-10-02 | 2,3,4                     |
| 2020-10-03 | 1,5,6                     |
| 2020-10-01 | 2,3,5                     |
+------------+---------------------------+
```

Calculate the number of same `user_id` corresponding to `'2020-10-01'` and `'2020-10-02'`. Two same elements (2 and 3) are found and the number `2` is returned.

```plaintext
mysql>  select intersect_count(user_id, dt, '2020-10-01', '2020-10-02')
        from tbl
        where dt in ('2020-10-01', '2020-10-02');
+----------------------------------------------------------+
| intersect_count(user_id, dt, '2020-10-01', '2020-10-02') |
+----------------------------------------------------------+
|                                                        2 |
+----------------------------------------------------------+
```

Calculate the number of same `user_id` corresponding to `'2020-10-01'` and `'2020-10-03'`. One element `5` is found and the number `1` is returned.

```plaintext
mysql>  select intersect_count(user_id, dt, '2020-10-01', '2020-10-03')
        from tbl
        where dt in ('2020-10-01', '2020-10-03');
+----------------------------------------------------------+
| intersect_count(user_id, dt, '2020-10-01', '2020-10-03') |
+----------------------------------------------------------+
|                                                        1 |
+----------------------------------------------------------+
```
