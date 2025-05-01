---
displayed_sidebar: docs
---

# percentile_cont

## Description

`expr` のパーセンタイル値を線形補間で計算します。

## Syntax

```Haskell
PERCENTILE_CONT (expr, percentile) 
```

## Parameters

- `expr`: 値を順序付けるための式です。数値データ型、DATE、または DATETIME でなければなりません。例えば、物理の中央値を見つけたい場合は、物理のスコアを含む列を指定します。

- `percentile`: 見つけたい値のパーセンタイルです。0 から 1 までの定数浮動小数点数です。例えば、中央値を見つけたい場合、このパラメータを `0.5` に設定します。

## Return value

指定されたパーセンタイルにある値を返します。入力値がちょうど希望するパーセンタイルにない場合、最も近い2つの入力値の線形補間を使用して結果が計算されます。

データ型は `expr` と同じです。

## Usage notes

この関数は NULL を無視します。

## Examples

`exam` という名前のテーブルが以下のデータを持っているとします。

```Plain
select * from exam order by Subject;
+-----------+-------+
| Subject   | Score |
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

NULL を無視して各科目の中央値を計算します。

Query:

```SQL
SELECT Subject, PERCENTILE_CONT (Score, 0.5)  FROM exam group by Subject;
```

Result:

```Plain
+-----------+-----------------------------+
| Subject   | percentile_cont(Score, 0.5) |
+-----------+-----------------------------+
| chemistry |                          90 |
| math      |                          70 |
| physics   |                        82.5 |
+-----------+-----------------------------+
```