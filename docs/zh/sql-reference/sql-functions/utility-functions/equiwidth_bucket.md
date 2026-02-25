---
displayed_sidebar: docs
---

# equiwidth_bucket

计算等宽直方图的桶。

## 语法

```SQL
equiwidth_bucket(value, min, max, buckets) 
```

## 参数

- `value`: 行值，必须在 `[min, max]` 之间
- `min`: 直方图的最小值
- `max`: 直方图的最大值
- `buckets`: 直方图的桶数


## 示例

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