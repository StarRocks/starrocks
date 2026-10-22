---
displayed_sidebar: docs
description: "DOUBLE値をPERCENTILE値として構築します。"
---

# percentile_hash

DOUBLE 値を PERCENTILE 値として構築します。

## Syntax

```Haskell
PERCENTILE_HASH(x[, compression]);
```

## Parameters

`x`: サポートされているデータ型は DOUBLE です。

`compression`: [2048, 10000] の範囲内の整数値となる省略可能な定数式です。評価結果が整数であれば、`5000.0` のような小数表記、キャスト、定数の算術式を使用できます。小数部分を持つ値や定数でない式はエラーになります。明示的な `NULL` と範囲外の整数には `10000` を使用します。引数を省略すると従来の圧縮パラメータ `1000` を維持します。

2 引数形式を使用する前に、すべての BE と CN ノードを、この形式に対応したバージョンへアップグレードしてください。

## Return value

PERCENTILE 値を返します。

## Examples

```Plain Text
mysql> select percentile_approx_raw(percentile_hash(234.234), 0.99);
+-------------------------------------------------------+
| percentile_approx_raw(percentile_hash(234.234), 0.99) |
+-------------------------------------------------------+
|                                    234.23399353027344 |
+-------------------------------------------------------+
1 row in set (0.00 sec)
```