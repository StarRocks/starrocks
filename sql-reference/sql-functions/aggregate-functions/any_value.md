# ANY_VALUE

## 功能

用于包含 `GROUP BY` 的聚合查询中，利用这个函数可以在聚合语句中选择未参与聚合运算的列，它将从每个聚合分组中 **随机** 选择一行返回.

## 语法

```Haskell
ANY_VALUE(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

在每个聚合后的分组中 **随机** 选择某行的结果返回，结果是不确定的.

## 示例

```plain text
// original data
mysql> select * from any_value_test;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |    1 |    1 |
|    1 |    2 |    1 |
|    2 |    1 |    1 |
|    2 |    2 |    2 |
|    3 |    1 |    1 |
+------+------+------+
5 rows in set (0.01 sec)

// after use ANY_VALUE
mysql> select a,any_value(b),sum(c) from any_value_test group by a;
+------+----------------+----------+
| a    | any_value(`b`) | sum(`c`) |
+------+----------------+----------+
|    1 |              1 |        2 |
|    2 |              1 |        3 |
|    3 |              1 |        1 |
+------+----------------+----------+
3 rows in set (0.01 sec)

mysql> select c,any_value(a),sum(b) from any_value_test group by c;
+------+----------------+----------+
| c    | any_value(`a`) | sum(`b`) |
+------+----------------+----------+
|    1 |              1 |        5 |
|    2 |              2 |        2 |
+------+----------------+----------+
2 rows in set (0.01 sec)

```
