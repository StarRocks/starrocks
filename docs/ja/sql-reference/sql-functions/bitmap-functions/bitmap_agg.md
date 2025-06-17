---
displayed_sidebar: docs
---

# bitmap_agg

列内の値（NULLを除く）をビットマップに集約します（複数の行を1つの行にまとめます）。

## 構文

```Haskell
BITMAP_AGG(col)
```

## パラメータ

`col`: 集約したい列の値。BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINTに評価される必要があります。

## 戻り値

BITMAP型の値を返します。

## 使用上の注意

行の値が0未満または18446744073709551615を超える場合、その値は無視され、ビットマップに追加されません（例3を参照）。

## 例

以下のデータテーブルを例として考えます。

```PlainText
mysql> CREATE TABLE t1_test (
    c1 int,
    c2 boolean,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "3");

INSERT INTO t1_test VALUES
    (1, true, 11, 111, 1111, 11111),
    (2, false, 22, 222, 2222, 22222),
    (3, true, 33, 333, 3333, 33333),
    (4, null, null, null, null, null),
    (5, -1, -11, -111, -1111, -11111),
    (6, null, null, null, null, "36893488147419103232");

select * from t1_test order by c1;
+------+------+------+------+-------+----------------------+
| c1   | c2   | c3   | c4   | c5    | c6                   |
+------+------+------+------+-------+----------------------+
|    1 |    1 |   11 |  111 |  1111 | 11111                |
|    2 |    0 |   22 |  222 |  2222 | 22222                |
|    3 |    1 |   33 |  333 |  3333 | 33333                |
|    4 | NULL | NULL | NULL |  NULL | NULL                 |
|    5 |    1 |  -11 | -111 | -1111 | -11111               |
|    6 | NULL | NULL | NULL |  NULL | 36893488147419103232 |
+------+------+------+------+-------+----------------------+
```

例1: 列 `c1` の値を1つのビットマップに集約します。

```PlainText
mysql> select bitmap_to_string(bitmap_agg(c1)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c1)) |
+----------------------------------+
| 1,2,3,4,5,6                      |
+----------------------------------+
```

例2: 列 `c2` の値を1つのビットマップに集約します（NULLは無視されます）。

```PlainText
mysql> SELECT BITMAP_TO_STRING(BITMAP_AGG(c2)) FROM t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c2)) |
+----------------------------------+
| 0,1                              |
+----------------------------------+
```

例3: 列 `c6` の値を1つのビットマップに集約します（値の範囲を超える最後の2つの値は無視されます）。

```PlainText
mysql> select bitmap_to_string(bitmap_agg(c6)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c6)) |
+----------------------------------+
| 11111,22222,33333                |
+----------------------------------+
```

## キーワード

BITMAP_AGG, BITMAP