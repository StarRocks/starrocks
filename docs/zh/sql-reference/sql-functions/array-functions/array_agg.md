---
displayed_sidebar: docs
---

# array_agg

## 功能

将一列中的值（包括空值 null）串联成一个数组（多行转一行）。在 3.0 版本之前，该函数不保证数组里元素的顺序。从 3.0 版本开始，array_agg() 支持使用 ORDER BY 对数组里的元素进行排序。

## 语法

```Haskell
ARRAY_AGG([distinct] col [order by col0 [desc | asc] [nulls first | nulls last] ...])
```

## 参数说明

- `col`：需要进行数值串联的列。支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、CHAR、DATETIME、DATE、ARRAY (3.1 及以后)、MAP (3.1 及以后)、STRUCT (3.1 及以后)。

- `col0`: 排序列，决定 `col` 中元素的顺序。可以有多个排序列。

- `[desc | asc]`: 对数组元素进行排序时，是基于 `col0` 的升序还是降序进行排列。默认升序。

- `[nulls first | nulls last]`: null 值排在元素最前面还是最后面。

## 返回值说明

返回值的数据类型为 ARRAY。

## 注意事项

- 如果不指定 ORDER BY，数组中元素的顺序是随机的，不能保证与原来列值的顺序相同。
- 返回数组中元素的类型与 `col` 类型一致。
- 如果没有满足条件的输入值，返回 NULL。
- ARRAY_AGG_DISTINCT() 是 ARRAY_AGG(DISTINCT) 的别名。

## 示例

对于整行进行转换时，如果没有满足条件的数据，聚合结果为 `NULL`。

```Plain Text
mysql> select array_agg(c2) from test where c1>4;
+-----------------+
| array_agg(`c2`) |
+-----------------+
| NULL            |
+-----------------+
```

## Examples

下面的示例使用如下数据表进行介绍：

```Plain_Text
mysql> select * from t;
+------+------+------+
| a    | name | pv   |
+------+------+------+
|   11 |      |   33 |
|    2 | NULL |  334 |
|    1 | fzh  |    3 |
|    1 | fff  |    4 |
|    1 | fff  |    5 |
+------+------+------+
```

示例一: 根据 `a` 列分组，将 `pv` 列的值串联成数组，数组元素基于 `name` 的升序进行排序，null 值排在最前。

```Plain_Text
mysql> select a, array_agg(pv order by name nulls first) from t group by a;
+------+---------------------------------+
| a    | array_agg(pv ORDER BY name ASC) |
+------+---------------------------------+
|    2 | [334]                           |
|   11 | [33]                            |
|    1 | [4,5,3]                         |
+------+---------------------------------+

-- 不使用排序。
mysql> select a, array_agg(pv) from t group by a;
+------+---------------+
| a    | array_agg(pv) |
+------+---------------+
|   11 | [33]          |
|    2 | [334]         |
|    1 | [3,4,5]       |
+------+---------------+
3 rows in set (0.03 sec)
```

示例二: 将 `pv` 列的值串联成数组，数组元素基于 `name` 的降序进行排序，null 值排在最后。

```Plain_Text
mysql> select array_agg(pv order by name desc nulls last) from t;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| [3,4,5,33,334]                   |
+----------------------------------+
1 row in set (0.02 sec)

-- 不使用排序。
mysql> select array_agg(pv) from t;
+----------------+
| array_agg(pv)  |
+----------------+
| [3,4,5,33,334] |
+----------------+
1 row in set (0.03 sec)
```

示例三: 将 `pv` 列的值串联成数组，使用 where 子句对 `pv` 值进行过滤。没有满足条件的值，返回 NULL。

```Plain_Text
mysql> select array_agg(pv order by name desc nulls last) from t where a < 0;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| NULL                             |
+----------------------------------+
1 row in set (0.02 sec)

-- Aggregate values with no order.
mysql> select array_agg(pv) from t where a < 0;
+---------------+
| array_agg(pv) |
+---------------+
| NULL          |
+---------------+
1 row in set (0.03 sec)
```

## Keywords

ARRAY_AGG, ARRAY
