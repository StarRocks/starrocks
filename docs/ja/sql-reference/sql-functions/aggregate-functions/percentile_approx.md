---
displayed_sidebar: docs
---

# percentile_approx

p 番目のパーセンタイルの近似値を返します。ここで、p の値は 0 から 1 の間です。

圧縮パラメータはオプションであり、設定範囲は [2048, 10000] です。値が大きいほど精度が高くなり、メモリ消費量が増え、計算時間が長くなります。指定されていない場合、または [2048, 10000] の範囲を超えていない場合、関数はデフォルトの圧縮パラメータ 10000 で実行されます。

この関数は固定サイズのメモリを使用するため、高いカーディナリティを持つ列に対して少ないメモリを使用でき、tp99 などの統計を計算するために使用できます。

## 構文

```Haskell
PERCENTILE_APPROX(expr, DOUBLE p[, DOUBLE compression])
```

## 例

```plain text
MySQL > select `table`, percentile_approx(cost_time,0.99)
from log_statis
group by `table`;
+----------+--------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+

MySQL > select `table`, percentile_approx(cost_time,0.99, 4096)
from log_statis
group by `table`;
+----------+----------------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99, 4096.0) |
+----------+----------------------------------------------+
| test     |                                        54.21 |
+----------+----------------------------------------------+
```

## キーワード

PERCENTILE_APPROX,PERCENTILE,APPROX