---
displayed_sidebar: docs
---

# equiwidth_bucket

Calculate the equi-width histogram bucket.

## Syntax

```SQL
equiwidth_bucket(value, min, max, buckets) 
```

## Parameters

- `value`: row value, must be within the `[min, max]`
- `min`: min value of the histogram
- `max`: max value of the histogram
- `buckets`: number of histogram buckets


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