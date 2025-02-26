---
displayed_sidebar: docs
---

# intersect_count

2 つの Bitmap 値の共通部分のサイズ（同じ要素の数）を求める関数で、データの分布が直交する必要はありません。最初のパラメータは Bitmap 列、2 番目のパラメータはフィルタリングに使用する次元列、3 番目のパラメータは可変長パラメータで、フィルタリング次元列の異なる値が取られます。

共通部分がない場合は、0 が返されます。

## 構文

```Haskell
BIGINT INTERSECT_COUNT(bitmap_column, column_to_filter, filter_values)
```

## パラメータ

- `bitmap_column`: 計算対象の bitmap 列。
- `column_to_filter`: 共通部分を求める列の名前。
- `filter_values`: フィルタリングされた次元列の異なる値。

## 戻り値

BIGINT 型の値を返します。

## 例

Bitmap 列 `user_id` を持つ集計テーブルを作成し、データを挿入します。

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

`'2020-10-01'` と `'2020-10-02'` に対応する同じ `user_id` の数を計算します。同じ要素が 2 つ（2 と 3）見つかり、数値 `2` が返されます。

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

`'2020-10-01'` と `'2020-10-03'` に対応する同じ `user_id` の数を計算します。1 つの要素 `5` が見つかり、数値 `1` が返されます。

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