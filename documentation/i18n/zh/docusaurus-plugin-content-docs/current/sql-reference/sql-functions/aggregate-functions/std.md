# STD

## 功能

返回 `expr` 表达式的标准差。从 2.5.10 版本开始，该函数也可以用作窗口函数。

## 语法

```Haskell
STD(expr)
```

## 参数说明

`expr`: 选取的表达式。当表达式为表中一列时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL。

## 返回值说明

返回 DOUBLE 类型的值。

## 示例

示例数据集：

```plaintext
MySQL [test]> select * from std_test;
+------+------+
| col0 | col1 |
+------+------+
|    0 |    0 |
|    1 |    2 |
|    2 |    4 |
|    3 |    6 |
|    4 |    8 |
+------+------+
```

计算 `col0` 和 `col1` 的标准差。

```plaintext
MySQL > select std(col0) as std_of_col0, std(col1) as std_of_col1 from std_test;
+--------------------+--------------------+
| std_of_col0        | std_of_col1        |
+--------------------+--------------------+
| 1.4142135623730951 | 2.8284271247461903 |
+--------------------+--------------------+
```

## Keywords

STD
