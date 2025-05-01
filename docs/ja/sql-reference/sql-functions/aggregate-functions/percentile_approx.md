---
displayed_sidebar: docs
---

# percentile_approx

## Description

0 から 1 の間の p の値に対する p パーセンタイルの近似値を返します。

圧縮パラメータはオプションで、設定範囲は [2048, 10000] です。値が大きいほど精度が高くなり、メモリ消費が増え、計算時間が長くなります。指定されていない場合や [2048, 10000] の範囲を超えない場合、関数はデフォルトの圧縮パラメータ 10000 で実行されます。

この関数は固定サイズのメモリを使用するため、高いカーディナリティを持つ列に対してメモリを少なく使用でき、tp99 などの統計を計算するのに使用できます。

## Syntax

```Haskell
PERCENTILE_APPROX(expr, DOUBLE p[, DOUBLE compression])
```

## Examples

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

## keyword

PERCENTILE_APPROX,PERCENTILE,APPROX