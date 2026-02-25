---
displayed_sidebar: docs
---

# equiwidth_bucket

等幅ヒストグラムのバケットを計算します。

## Syntax

```SQL
equiwidth_bucket(value, min, max, buckets) 
```

## Parameters

- `value`: 行の値で、`[min, max]` の範囲内でなければなりません
- `min`: ヒストグラムの最小値
- `max`: ヒストグラムの最大値
- `buckets`: ヒストグラムのバケット数

## Example

```SQL
MYSQL > select r, equiwidth_bucket(r, 0, 10, 20) as x from table(generate_series(0, 10)) as s(r);

0	0
1	1
2	2
3	3
4	4
5	5
6	6
7	7
8	8
9	9
10	10
```