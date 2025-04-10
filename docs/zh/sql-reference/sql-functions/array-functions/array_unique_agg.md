---
displayed_sidebar: docs
---

# array_unique_agg

## 功能

将一列中的 distinct 值（包括空值 null）串联成一个数组（多行转一行）。

该函数从 3.2 版本开始支持。

## 语法

```Haskell
ARRAY_UNIQUE_AGG(col)
```

## 参数说明

- `col`：需要进行数值串联的列。支持的数据类型为 ARRAY。

## 返回值说明

返回值的数据类型为 ARRAY。

## 注意事项

- 数组中元素的顺序是随机的。
- 返回数组中元素的类型与 `col` 元素类型一致。
- 如果没有满足条件的输入值，返回 NULL。

## 示例

下面的示例使用如下数据表进行介绍：

```plaintext
mysql > select * from array_unique_agg_example;
+------+--------------+
| a    | b            |
+------+--------------+
|    2 | [1,null,2,4] |
|    2 | [1,null,3]   |
|    1 | [1,1,2,3]    |
|    1 | [2,3,4]      |
+------+--------------+
```

示例一: 根据 `a` 列分组，将 `b` 列的值串联成数组。

```plaintext
mysql > select a, array_unique_agg(b) from array_unique_agg_example group by a;
+------+---------------------+
| a    | array_unique_agg(b) |
+------+---------------------+
|    1 | [4,1,2,3]           |
|    2 | [4,1,2,3,null]      |
+------+---------------------+
```

示例二: 将 `b` 列的值串联成数组，使用 WHERE 子句进行过滤。没有满足条件的值，返回 NULL。

```plaintext
mysql > select array_unique_agg(b) from array_unique_agg_example where a < 2;
+---------------------+
| array_unique_agg(b) |
+---------------------+
| [4,1,2,3]           |
+---------------------+

mysql > select array_unique_agg(b) from array_unique_agg_example where a < 0;
+---------------------+
| array_unique_agg(b) |
+---------------------+
| NULL                |
+---------------------+
```

## Keywords

ARRAY_UNIQUE_AGG, ARRAY
