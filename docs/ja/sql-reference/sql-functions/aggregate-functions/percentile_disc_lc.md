---
displayed_sidebar: docs
---

# percentile_disc_lc

入力列 `expr` の離散分布に基づいてパーセンタイル値を返します。percentile_disc と同じ動作ですが、実装アルゴリズムが異なります。percentile_disc はすべての入力データを取得する必要があり、パーセンタイル値を取得するためのマージソートに消費されるメモリはすべての入力データのメモリです。一方、percentile_disc_lc は key->count のハッシュテーブルを構築するため、入力のカーディナリティが低い場合、入力データサイズが大きくても明らかなメモリ増加はありません。

この関数は v3.4 以降でサポートされています。

## 構文

```SQL
PERCENTILE_DISC_LC (expr, percentile) 
```

## パラメータ

- `expr`: パーセンタイル値を計算したい列。この列はソート可能な任意のデータ型であることができます。
- `percentile`: 求めたい値のパーセンタイル。0 から 1 の間の定数の浮動小数点数である必要があります。たとえば、中央値を求めたい場合、このパラメータを `0.5` に設定します。70 パーセンタイルの値を求めたい場合は、0.7 を指定します。

## 戻り値

戻り値のデータ型は `expr` と同じです。

## 使用上の注意

計算では NULL 値は無視されます。

## 例

テーブル `exam` を作成し、このテーブルにデータを挿入します。

```sql
CREATE TABLE exam (
    subject STRING,
    score INT
) 
DISTRIBUTED BY HASH(`subject`);

INSERT INTO exam VALUES
('chemistry',80),
('chemistry',100),
('chemistry',null),
('math',60),
('math',70),
('math',85),
('physics',75),
('physics',80),
('physics',85),
('physics',99);
```

```Plain
select * from exam order by subject;
+-----------+-------+
| subject   | score |
+-----------+-------+
| chemistry |    80 |
| chemistry |   100 |
| chemistry |  NULL |
| math      |    60 |
| math      |    70 |
| math      |    85 |
| physics   |    75 |
| physics   |    80 |
| physics   |    85 |
| physics   |    99 |
+-----------+-------+
```

各科目の中央値を計算します。

```SQL
select subject, percentile_disc_lc (score, 0.5)
from exam group by subject;
```

出力

```Plain
+-----------+--------------------------------+
| subject   | percentile_disc_lc(score, 0.5) |
+-----------+--------------------------------+
| physics   |                             85 |
| chemistry |                            100 |
| math      |                             70 |
+-----------+--------------------------------+
```