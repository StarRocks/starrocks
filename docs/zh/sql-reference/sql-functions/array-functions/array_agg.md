---
displayed_sidebar: "Chinese"
---

# array_agg

## 功能

将一列中的值（包括空值 null）串联成一个数组，可以用于多行转一行（行转列）。

## 语法

```Haskell
ARRAY_AGG(col)
```

## 参数说明

`col`：需要转换的列。支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、CHAR、DATETIME、DATE。

## 返回值说明

返回的数据类型为 ARRAY。

## 注意事项

* 数组中元素不保证顺序。
* 返回转换生成的数组。数组中的元素类型与 `col` 类型一致。

## 示例

下面的示例使用如下数据表进行介绍。

```Plain Text
mysql> select * from test;
+------+------+
| c1   | c2   |
+------+------+
|    1 | a    |
|    1 | b    |
|    2 | c    |
|    2 | NULL |
|    3 | NULL |
+------+------+
```

根据 `c1` 列分组，对 `c2` 执行**行转列**。

```Plain Text
mysql> select c1, array_agg(c2) from test group by c1;
+------+-----------------+
| c1   | array_agg(`c2`) |
+------+-----------------+
|    1 | ["a","b"]       |
|    2 | [null,"c"]      |
|    3 | [null]          |
+------+-----------------+
```

对于整行进行转换时，如果没有满足条件的数据，聚合结果为 `NULL`。

```Plain Text
mysql> select array_agg(c2) from test where c1>4;
+-----------------+
| array_agg(`c2`) |
+-----------------+
| NULL            |
+-----------------+
```
