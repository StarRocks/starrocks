---
displayed_sidebar: "Chinese"
---

# std

## 功能

返回指定列的标准差。

## 语法

```Haskell
STD(expr)
```

## 返回值说明

使用 DOUBLE 数据类型返回列的标准差。

## 示例

示例数据集：

```plain
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

```sql
MySQL > select std(col0) as std_of_col0, std(col1) as std_of_col1 from std_test;
+--------------------+--------------------+
| std_of_col0        | std_of_col1        |
+--------------------+--------------------+
| 1.4142135623730951 | 2.8284271247461903 |
+--------------------+--------------------+
```

## 关键词

STD
