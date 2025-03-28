---
displayed_sidebar: docs
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

Fast Decimal は FE の動的パラメータ `enable_decimal_v3` によって制御され、デフォルトでは `true` です。

バージョン 3.1 以降、StarRocks は [ARRAY](../semi_structured/Array.md)、[MAP](../semi_structured/Map.md)、および [STRUCT](../semi_structured/STRUCT.md) における Fast Decimal エントリをサポートしています。

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

decimal, decimalv2, decimalv3, fast decimal