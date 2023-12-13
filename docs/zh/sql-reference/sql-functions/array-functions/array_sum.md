---
displayed_sidebar: "Chinese"
---

# array_sum

## description

### Syntax

```Haskell
array_sum(array(type))
```

对一个ARRAY中的所有数据做和，返回这个结果。

## example

```plain text
mysql> select array_sum([11, 11, 12]);
+-----------------------+
| array_sum([11,11,12]) |
+-----------------------+
| 34                    |
+-----------------------+

mysql> select array_sum([11.33, 11.11, 12.324]);
+---------------------------------+
| array_sum([11.33,11.11,12.324]) |
+---------------------------------+
| 34.764                          |
+---------------------------------+
```

## keyword

ARRAY_SUM,ARRAY
