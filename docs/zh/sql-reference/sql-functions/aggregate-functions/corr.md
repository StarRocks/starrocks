# CORR

## 功能

返回两个随机变量的皮尔逊相关系数（correlation coefficient）。该函数从 2.5.10 版本开始支持，也可用作窗口函数。

## 语法

```Haskell
CORR(expr1, expr2)
```

## 参数说明

`expr1`: 选取的表达式 1。

`expr2`: 选取的表达式 2。

当表达式为表中一列时，用于计算两列值之间的相关系数。支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL。

## 返回值说明

返回值为 DOUBLE 类型。计算公式如下，其中 `n` 为该表的行数：

![corr_formula](../../../assets/corr_formula.png)

<!--$$
\frac{\sum_{i=1}^{n}((x_i - \bar{x})(y_i - \bar{y}))}{\sqrt{\sum_{i=1}^{n}((x_i - \bar{x})^2) \cdot \sum_{i=1}^{n}((y_i - \bar{y})^2)}}
$$ -->

## 使用说明

* 计算相关系数时，只有同一行的两列数据都不为 null 时，该行数据才会被统计到最终结果中，否则该行数据会被忽略。

* 当 `n` 为 1 时，返回值为 0。

## 示例

对于以下数据表：

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

计算列 `k` 和 列 `v` 的相关系数:

```plaintext
mysql> select no,CORR(k,v) from agg group by no;
+------+--------------------+
| no   | corr(k, v)         |
+------+--------------------+
|    1 |               NULL |
|    2 | 0.9988445981121532 |
+------+--------------------+
```
