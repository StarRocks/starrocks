---
displayed_sidebar: docs
---

# bar

ヒストグラムのような棒グラフを描いて、データ分布を確認します。

## Syntax

```SQL
bar(size, min, max, width) 
```

## Parameters

- `size`: 棒のサイズで、`[min, max]` の範囲内でなければなりません
- `min`: 棒の最小値
- `max`: 棒の最大値
- `width`: 棒の幅

## Example

```SQL
MYSQL > select r, bar(r, 0, 10, 20) as x from table(generate_series(0, 10)) as s(r);

0	
1	▓▓
2	▓▓▓▓
3	▓▓▓▓▓▓
4	▓▓▓▓▓▓▓▓
5	▓▓▓▓▓▓▓▓▓▓
6	▓▓▓▓▓▓▓▓▓▓▓▓
7	▓▓▓▓▓▓▓▓▓▓▓▓▓▓
8	▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
9	▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
10	▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓

```