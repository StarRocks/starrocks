---
displayed_sidebar: docs
---

# DECIMAL

DECIMAL(P[,S]) は高精度の固定小数点値です。`P` は有効桁数（精度）を表します。`S` は小数点以下の最大桁数（スケール）を表します。

`P` が省略された場合、デフォルト値は10です。`S` が省略された場合、デフォルト値は0です。

- バージョン v4.0 より前のバージョンでは、StarRocks は [Fast DECIMAL](#fast-decimal) に基づく DECIMAL128 をサポートしています。
- バージョン v4.0 以降では、StarRocks は [DECIMAL256](#decimal256) をサポートしています。

## 機能

### Fast DECIMAL

FE の動的パラメータ `enable_decimal_v3` によって制御され、Fast DECIMAL はデフォルトで有効になっています。

Fast DECIMAL の場合、`P` の範囲は [1,38] で、`S` の範囲は [0, P] です。`S` のデフォルト値は0です。

Fast DECIMAL は可変幅の整数を使用して小数を表現します。

- Decimal(P &le; 18, S)
  - LogicalType: Decimal64
  - 保存形式: int64
  - Delegate LogicalType: BIGINT

- Decimal(P &gt; 18 & P &le; 38, S)
  - LogicalType: Decimal128
  - 保存形式: int128
  - Delegate LogicalType: LARGEINT

バージョン v3.1 以降、StarRocks は [ARRAY](../semi_structured/Array.md)、[MAP](../semi_structured/Map.md)、および [STRUCT](../semi_structured/STRUCT.md) における Fast Decimal エントリをサポートしています。

## DECIMAL256

StarRocks はバージョン v4.0 以降で DECIMAL256 を導入しています。

DECIMAL256 の場合、`P` の範囲は (38,76] で、`S` の範囲は [0, P] です。`S` のデフォルト値は0です。

DECIMAL256 は精度の上限を76ビットに拡張します。その数値容量は DECIMAL128 の 10³⁸ 倍であり、オーバーフローの可能性を無視できるレベルにまで低減します。

DECIMAL256 は Fast DECIMAL と同じ戦略を使用して小数を表現します。上記の2つの小数タイプに加えて、次のものをサポートします：

- Decimal(P &gt; 38 & P &le; 76, S)
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

## 例

### Fast DECIMAL の例

テーブルを作成する際に DECIMAL カラムを定義します。

```SQL
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

### DECIMAL256 の例

DECIMAL256 タイプのカラムを持つテーブルを作成します。

```SQL
CREATE TABLE test_decimal256(
    p50s48 DECIMAL(50, 48) COMMENT ""
);
```

異なる値を挿入します：

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 1.222222;
```

このステートメントは、値が正しく書き込まれて成功します。

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 11111111111111111111111111111111111111111111.222222;
```

このステートメントは、値が NULL として書き込まれて成功します。整数部分と小数部分（44桁）が指定されたスケール（`48`）と共に、256ビットで表現可能な最大整数を超えているため、FE は直接値を NULL にキャストします。

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 333;
```

このステートメントはエラー `ERROR 1064 (HY000): Insert has filtered data` と共に返されます。整数部分と小数部分が指定されたスケール（`48`）と共に、50桁の整数で表現される最大値を超えているため、BE はエラーを返します。

## 使用上の注意

システム変数 `sql_mode` を `ERROR_IF_OVERFLOW` に設定することで、算術オーバーフローの場合にシステムが NULL の代わりにエラーを返すようにすることができます。

## キーワード

decimal, decimalv3, fast decimal, decimal128, decimal256