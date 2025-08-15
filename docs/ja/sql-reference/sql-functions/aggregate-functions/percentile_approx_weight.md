# percentile_approx_weight

重み付きの p パーセンタイルの近似値を返します。`percentile_approx_weight` は `PERCENTILE_APPROX` の重み付きバージョンであり、各入力値に対して重み（定数値または数値列）を指定することができます。

この関数は固定サイズのメモリを使用するため、高いカーディナリティを持つ列に対してメモリ使用量を抑えることができます。また、tp99 などの統計を計算するためにも使用できます。

## 構文

```Haskell
DOUBLE PERCENTILE_APPROX_WEIGHT(expr, BIGINT weight, DOUBLE p[, DOUBLE compression])
```

- `expr`: パーセンタイルを計算する列。
- `pth` : パーセンタイル値。範囲: [0, 1]。例えば、`0.99` は 99 パーセンタイルを示します。
- `weight` : 重み列。正の定数値または列でなければなりません。
- `compression` : (オプション) 圧縮率。範囲: [2048, 10000]。値が大きいほど精度が高くなり、メモリ消費量が増え、計算時間が長くなります。このパラメータが指定されていない場合、または範囲 [2048, 10000] を超える場合、デフォルト値 `10000` が使用されます。

## 例

```plain text
CREATE TABLE t1 (
    c1 int,
    c2 double,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string,
    c8 double,
    c9 date,
    c10 datetime,
    c11 array<int>,
    c12 map<double, double>,
    c13 struct<a bigint, b double>
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 
    select generate_series, generate_series,  11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)
    from table(generate_series(1, 50000, 3));
-- 定数値を重みとして使用
mysql> select percentile_approx_weighted(c1, 1, 0.9) from t1;
+----------------------------------------+
| percentile_approx_weighted(c1, 1, 0.9) |
+----------------------------------------+
|                         45000.39453125 |
+----------------------------------------+
1 row in set (0.07 sec)
-- 数値列を重みとして使用
mysql> select percentile_approx_weighted(c2, c1, 0.5) from t1;
+-----------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5) |
+-----------------------------------------+
|                              35355.9375 |
+-----------------------------------------+
1 row in set (0.07 sec)
-- 重みと圧縮を使用してパーセンタイルを計算
mysql> select percentile_approx_weighted(c2, c1, 0.5, 10000) from t1;
+------------------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5, 10000) |
+------------------------------------------------+
|                                     35355.9375 |
+------------------------------------------------+
1 row in set (0.09 sec)
```

## キーワード

PERCENTILE_APPROX_WEIGHT,PERCENTILE_APPROX,PERCENTILE,APPROX
