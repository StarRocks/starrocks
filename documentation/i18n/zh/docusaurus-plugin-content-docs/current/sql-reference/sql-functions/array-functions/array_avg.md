# array_avg

## 功能

求取一个ARRAY中的所有数据的平均数，返回这个结果。

## 语法

```Haskell
array_avg(array(type))
```

## 参数说明

`array(type)` 中的 `type` 支持如下类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2。

## 示例

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
