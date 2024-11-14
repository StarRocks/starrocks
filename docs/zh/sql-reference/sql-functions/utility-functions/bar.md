---
displayed_sidebar: docs
---

# bar

绘制类似直方图的条形图，以检查数据分布。

## 语法

```SQL
bar(size, min, max, width) 
```

## 参数

- `size`: 条形的大小，必须在 `[min, max]` 之间
- `min`: 条形的最小值
- `max`: 条形的最大值
- `width`: 条形的宽度

## 示例

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
