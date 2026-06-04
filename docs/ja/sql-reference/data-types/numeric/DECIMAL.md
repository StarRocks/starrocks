---
displayed_sidebar: docs
description: "高精度固定小数点値で精度とスケールを指定可能。"
---

# DECIMAL

## 説明

DECIMAL(P[,S])

高精度の固定小数点値です。`P` は有効数字の総数（精度）を表します。`S` は小数点以下の最大桁数（スケール）を表します。

`P` を省略した場合、デフォルトは10です。`S` を省略した場合、デフォルトは0です。

* Decimal V2

  `P` の範囲は [1,27] で、`S` の範囲は [0,9] です。`P` は `S` の値以上でなければなりません。`S` のデフォルト値は0です。

* Fast Decimal (Decimal V3)

  `P` の範囲は [1,38] で、`S` の範囲は [0, P] です。`S` のデフォルト値は0です。Fast Decimal はより高い精度を提供します。
  
  主な最適化:
  
  ​1. Fast Decimal は可変幅の整数を使用して小数を表現します。例えば、精度が18以下の小数を表現するために64ビット整数を使用します。一方、Decimal V2 はすべての小数に対して128ビット整数を一様に使用します。64ビットプロセッサ上での算術演算や変換操作は、より少ない命令を使用し、パフォーマンスを大幅に向上させます。
  
  ​2. Decimal V2 と比較して、Fast Decimal はいくつかのアルゴリズム、特に乗算において大幅な最適化を行い、パフォーマンスを約4倍向上させました。

<<<<<<< HEAD
Fast Decimal は FE の動的パラメータ `enable_decimal_v3` によって制御され、デフォルトでは `true` です。

バージョン 3.1 以降、StarRocks は [ARRAY](../semi_structured/Array.md)、[MAP](../semi_structured/Map.md)、および [STRUCT](../semi_structured/STRUCT.md) における Fast Decimal エントリをサポートしています。
=======
- Decimal(P `<` 18, S)
  - LogicalType: Decimal64
  - 保存形式: int64
  - Delegate LogicalType: BIGINT

- Decimal(P `>` 18 & P `<` 38, S)
  - LogicalType: Decimal128
  - 保存形式: int128
  - Delegate LogicalType: LARGEINT

バージョン v3.1 以降、StarRocks は [ARRAY](../semi_structured/Array.md)、[MAP](../semi_structured/Map.md)、および [STRUCT](../semi_structured/STRUCT.md) における Fast Decimal エントリをサポートしています。

## DECIMAL256

StarRocks はバージョン v4.0 以降で DECIMAL256 を導入しています。

DECIMAL256 の場合、`P` の範囲は (38,76] で、`S` の範囲は [0, P] です。`S` のデフォルト値は0です。

DECIMAL256 は精度の上限を76ビットに拡張します。その数値容量は DECIMAL128 の 10³⁸ 倍であり、オーバーフローの可能性を無視できるレベルにまで低減します。

DECIMAL256 は Fast DECIMAL と同じ戦略を使用して小数を表現します。上記の2つの小数タイプに加えて、次のものをサポートします：

- Decimal(P `>` 38 & P `<` 76, S)
  - LogicalType: Decimal256
  - 保存形式: int256
  - Delegate LogicalType: INT_256

DECIMAL256 の実際の表現可能な範囲は `-57896044618658097711785492504343953926634992332820282019728792003956564819968` から `57896044618658097711785492504343953926634992332820282019728792003956564819967` です。

#### 型変換操作

DECIMAL256 は以下にリストされたすべての型との双方向の型変換をサポートします：

| ソースタイプ     | ターゲットタイプ                                                      |
| --------------- | ----------------------------------------------------------------- |
| TYPE_DECIMAL256 | BOOL                                                              |
| TYPE_DECIMAL256 | TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT |
| TYPE_DECIMAL256 | TYPE_FLOAT, TYPE_DOUBLE                                           |
| TYPE_DECIMAL256 | TYPE_VARCHAR                                                      |

#### 集計関数

現在、DECIMAL256 は次の集計関数をサポートしています：`COUNT`、`COUNT DISTINCT`、`SUM`、`SUM DISTINCT`、`AVG`、`AVG DISTINCT`、`MAX`、`MIN`、および `ABS`。

#### 制限事項

DECIMAL256 には以下の制限があります：

- 現在、DECIMAL256 は小数演算における自動精度スケールアップをサポートしていません。

  例えば、`DECIMAL128 * DECIMAL128` は自動的に DECIMAL256 にスケールアップされません。DECIMAL256 の機能は、`DECIMAL256` が CREATE TABLE ステートメントで明示的にオペランドとして指定されるか、CAST を使用する場合（`SELECT CAST(p38s10 as DECIMAL(70, 30))`）にのみ適用されます。

- DECIMAL256 はウィンドウ関数をサポートしていません。

- 集計テーブルは DECIMAL256 タイプをサポートしていません。
>>>>>>> e16d27a64e ([Doc] generate descriptions (#74345))

## 例

テーブルを作成する際に DECIMAL 列を定義します。

```sql
CREATE TABLE decimalDemo (
    pk BIGINT(20) NOT NULL COMMENT "",
    account DECIMAL(20,10) COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);

INSERT INTO decimalDemo VALUES
(1,3.141592656),
(2,21.638378),
(3,4873.6293048479);

SELECT * FROM decimalDemo;
+------+-----------------+
| pk   | account         |
+------+-----------------+
|    1 |    3.1415926560 |
|    3 | 4873.6293048479 |
|    2 |   21.6383780000 |
+------+-----------------+
```

## キーワード

<<<<<<< HEAD
decimal, decimalv2, decimalv3, fast decimal
=======
decimal, decimalv3, fast decimal, decimal128, decimal256
>>>>>>> e16d27a64e ([Doc] generate descriptions (#74345))
