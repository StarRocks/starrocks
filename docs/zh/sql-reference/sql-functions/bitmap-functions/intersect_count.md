---
displayed_sidebar: docs
---

# intersect_count

## 功能

计算两个 bitmap 之间相同元素的个数，不要求数据分布正交。第一个参数是 Bitmap 列，第二个参数是用来过滤的维度列，第三个参数是变长参数，含义是过滤维度列的不同取值。如果没有相同元素，则返回 0。

## 语法

```Haskell
BIGINT INTERSECT_COUNT(bitmap_column, column_to_filter, filter_values)
```

## 参数说明

- `bitmap_column`: 进行计算的 bitmap 列。
- `column_to_filter`: 需要进行交集计算的列名。
- `filter_values`: 过滤维度列的不同取值。

## 返回值说明

返回值的数据类型为 BIGINT。

## 示例

创建一张含 Bitmap 列的聚合表，并插入数据。

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

计算两个日期 `'2020-10-01'` 和 `'2020-10-02'` 里 `user_id` 相同的个数。有两个相同的 `user_id` (2 和 3)，返回个数 `2`。

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

计算日期 `'2020-10-01'` 和 `'2020-10-03'` 里 `user_id` 相同的个数。有 1 个相同的 `user_id` `5`，返回个数 `1`。

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
