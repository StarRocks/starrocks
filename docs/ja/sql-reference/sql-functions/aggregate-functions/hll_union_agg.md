---
displayed_sidebar: docs
---

# hll_union_agg

## Description

HLL は、HyperLogLog アルゴリズムに基づくエンジニアリング実装であり、HyperLogLog 計算プロセスの中間結果を保存するために使用されます。

これはテーブルの値カラムとしてのみ使用でき、集約を通じてデータ量を削減し、クエリの高速化を実現します。

HLL に基づく約 1% の誤差を持つ推定結果です。HLL カラムは、他のカラムから生成されるか、テーブルにロードされたデータに基づいて生成されます。

ロード中に、[hll_hash](../aggregate-functions/hll_hash.md) 関数を使用して、HLL カラムを生成するために使用されるカラムを指定します。これは、Count Distinct を置き換えるためによく使用され、ロールアップと組み合わせてビジネスで UV を迅速に計算するために使用されます。

## Syntax

```Haskell
HLL_UNION_AGG(hll)
```

## Examples

```plain text
MySQL > select HLL_UNION_AGG(uv_set) from test_uv;
+-------------------------+
| HLL_UNION_AGG(`uv_set`) |
+-------------------------+
| 17721                   |
+-------------------------+
```

## keyword

HLL_UNION_AGG,HLL,UNION,AGG