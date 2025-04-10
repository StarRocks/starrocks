---
displayed_sidebar: docs
---

# generate_series

## 功能

生成一系列从 `start` 到 `end` 的数值，步长为 `step`，`step` 默认为 1。

generate_series 是一个表函数。表函数为每个输入行返回一个行集合。返回的集合可以包含零行、一行或多行。每一行可以包含一个或多个列。

在 StarRocks 中调用 generate_series() 时，如果输入参数为数值常量，则需要使用 `TABLE()` 关键字包裹 generate_series()；如果输入参数为表达式，比如列名，则不需要使用 `TABLE()` 关键字包裹。具体参见[示例](#示例)。

该函数从 3.1 版本开始支持。

## 语法

```SQL
generate_series(start, end [,step])
```

## 参数说明

- `start`：起始值，必选。支持 INT、BIGINT、LARGEINT 类型。
- `end`：结束值，必选。支持 INT、BIGINT、LARGEINT 类型。
- `step`：数值递增或者递减的步长，可选。支持 INT、BIGINT、LARGEINT 类型。如果不指定，默认值为 1。`step` 取值不能为 0，否则报错。

三个参数的类型必须一致，比如 `generate_series(INT start, INT end [, INT step])`。

## 返回值说明

返回一系列数值，返回值的类型与参数 `start` 和 `end` 的类型相同。

- 当 `step` 为正数时，如果 `start` 大于 `end`，则返回零行。相反，当 `step` 为负数时，如果 `start` 小于 `end`，则返回零行。
- `step` 为 0 会返回错误。
- 对 NULL 值的处理：输入参数为 literal null 时，会返回报错；输入参数为表达式，表达式的结果为 NULL 时，会返回零行，参见示例五。

## 示例

示例一：升序返回 2 到 5 之间的整数值，使用默认步长。

```SQL
MySQL > select * from TABLE(generate_series(2, 5));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               3 |
|               4 |
|               5 |
+-----------------+
```

示例二：升序返回 2 和 5 之间的整数值，步长为 2。

```SQL
MySQL > select * from TABLE(generate_series(2, 5, 2));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               4 |
+-----------------+
```

示例三：降序返回 5 和 2 之间的数值，步长为 -1。

```SQL
MySQL > select * from TABLE(generate_series(5, 2, -1));
+-----------------+
| generate_series |
+-----------------+
|               5 |
|               4 |
|               3 |
|               2 |
+-----------------+
```

示例四：步长为负数，起始值小于结束值，返回零行。

```SQL
MySQL > select * from TABLE(generate_series(2, 5, -1));
Empty set (0.01 sec)
```

示例五：使用表中的列作为 generate_series() 的输入参数。注意此时**无需**使用 `TABLE()`。当输入行有 NULL 时，对于该行会返回零行，比如下表中的 `(NULL, 10)`。

```SQL
CREATE TABLE t_numbers(start INT, end INT)
DUPLICATE KEY (start)
DISTRIBUTED BY HASH(start) BUCKETS 1;

INSERT INTO t_numbers VALUES
(1, 3),
(5, 2),
(NULL, 10),
(4, 7),
(9,6);

SELECT * FROM t_numbers;
+-------+------+
| start | end  |
+-------+------+
|  NULL |   10 |
|     1 |    3 |
|     4 |    7 |
|     5 |    2 |
|     9 |    6 |
+-------+------+

-- 默认步长为 1，对数据行（1,3）和 (4,7) 升序返回多行。
SELECT * FROM t_numbers, generate_series(t_numbers.start, t_numbers.end);
+-------+------+-----------------+
| start | end  | generate_series |
+-------+------+-----------------+
|     1 |    3 |               1 |
|     1 |    3 |               2 |
|     1 |    3 |               3 |
|     4 |    7 |               4 |
|     4 |    7 |               5 |
|     4 |    7 |               6 |
|     4 |    7 |               7 |
+-------+------+-----------------+

-- 步长为 -1，对数据行（5,2）和 (9,6) 降序返回多行。
SELECT * FROM t_numbers, generate_series(t_numbers.start, t_numbers.end, -1);
+-------+------+-----------------+
| start | end  | generate_series |
+-------+------+-----------------+
|     5 |    2 |               5 |
|     5 |    2 |               4 |
|     5 |    2 |               3 |
|     5 |    2 |               2 |
|     9 |    6 |               9 |
|     9 |    6 |               8 |
|     9 |    6 |               7 |
|     9 |    6 |               6 |
+-------+------+-----------------+
```

## keywords

表格函数，表函数，table function，generate series
