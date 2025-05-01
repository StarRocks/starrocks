---
displayed_sidebar: docs
---

# percentile_disc

## 説明

入力列 `expr` の離散分布に基づいてパーセンタイル値を返します。正確なパーセンタイル値が見つからない場合、この関数は最も近い2つの値のうち大きい方の値を返します。

この関数はv2.5以降でサポートされています。

## 構文

```SQL
PERCENTILE_DISC (expr, percentile) 
```

## パラメータ

- `expr`: パーセンタイル値を計算したい列。この列はソート可能な任意のデータ型である必要があります。
- `percentile`: 見つけたい値のパーセンタイル。0から1の間の定数の浮動小数点数でなければなりません。例えば、中央値を見つけたい場合、このパラメータを `0.5` に設定します。70番目のパーセンタイルの値を見つけたい場合は、0.7を指定します。

## 戻り値

戻り値のデータ型は `expr` と同じです。

## 使用上の注意

計算にはNULL値は無視されます。

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
select subject, percentile_disc(score, 0.5)
from exam group by subject;
```

出力

```Plain
+-----------+-----------------------------+
| subject   | percentile_disc(score, 0.5) |
+-----------+-----------------------------+
| chemistry |                         100 |
| math      |                          70 |
| physics   |                          85 |
+-----------+-----------------------------+
```