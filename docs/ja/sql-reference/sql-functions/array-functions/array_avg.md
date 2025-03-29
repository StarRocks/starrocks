---
displayed_sidebar: docs
---

# array_avg

ARRAY 内のすべてのデータの平均値を計算し、この結果を返します。

## Syntax

```Haskell
array_avg(array(type))
```

`array(type)` は次のタイプの要素をサポートします: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMALV2.

## Examples

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