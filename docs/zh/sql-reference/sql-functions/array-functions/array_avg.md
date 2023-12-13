---
displayed_sidebar: "Chinese"
---

# array_avg

## description

### Syntax

```Haskell
array_avg(array(type))
```

求取一个ARRAY中的所有数据的平均数，返回这个结果。

## example

```plain text
mysql> select array_avg([11, 11, 12]);
+-----------------------+
| array_avg([11,11,12]) |
+-----------------------+
| 11.333333333333334    |
+-----------------------+

mysql> select array_avg([11.33, 11.11, 12.324]);
+---------------------------------+
| array_avg([11.33,11.11,12.324]) |
+---------------------------------+
| 11.588                          |
+---------------------------------+
```

## keyword

ARRAY_AVG,ARRAY
