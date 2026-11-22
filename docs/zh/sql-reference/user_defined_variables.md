---
displayed_sidebar: docs
description: "StarRocks 2.5+ supports user-defined variables to store session-level values for reuse in SQL statements."
products: [oss, enterprise, cloud, anywhere]
---

# 用户自定义变量

本文介绍如何声明和使用用户自定义变量。

自 2.5 版本起， StarRocks 支持用户声明自定义变量 (user-defined variables)。自定义变量用于存储特定的值，后续引用在 SQL 语句中，简化 SQL 语句的编写和避免重复计算。

## 使用说明

- 当前仅支持声明会话级别的自定义变量，即用户只能使用自己声明的自定义变量，且如客户端断开，那么当前会话中所有自定义变量将失效。
- StarRocks 暂不支持使用 SHOW 语句查看已有的自定义变量。
- 不支持声明 BITMAP、HLL 和 PERCENTILE 类型的自定义变量，JSON 类型的自定义变量会转换为 STRING 类型进行存储。
- 自 v3.2.7 起，StarRocks 支持声明 ARRAY 类型的自定义变量。

## 声明自定义变量

### 语法

```Plain
SET @var_name = expr [, ...];
```

> **说明**
>
> - 声明自定义变量时，必须加前缀 `@`。
> - 可同时声明多个自定义变量，多个变量之间用逗号 (,) 隔开。
> - 支持多次声明同一自定义变量，新声明的值会覆盖原有值。
> - 如在一个 SQL 语句中引用了没有声明过的变量，该变量值默认为 `NULL` 且为 STRING 类型。

### 参数说明

| **参数** | **必填** | **说明**                                                     |
| -------- | -------- | ------------------------------------------------------------ |
| var_name | 是       | 自定义变量名称，命名规则如下：<ul><li>必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (`_`) 组成。</li><li>总长度不能超过 64 个字符。</li></ul> 例如，@'my-var'、@"my-var" 和 @\`my-var\`。字符串类型的变量名称可以包含除字母、数字和下划线 (_) 以外的其他字符，例如句点 (.)。 |
| expr     | 是       | 自定义变量存储的值。支持简单值，例如 `43`；也支持复杂表达式，如 SELECT 查询返回的值。表达式计算结果的数据类型即为变量的数据类型。 |

### 示例

示例一：声明一个数值为自定义变量。

```SQL
SET @var = 43;
```

示例二：声明 SELECT 语句返回的值为自定义变量。

```SQL
SET @var = (SELECT SUM(v1) FROM test);
```

示例三：一次性声明多个变量。

```SQL
SET @v1=1, @v2=2;
```

## **使用场景**

- 简化 SQL 语句的编写。示例如下，执行 SELECT 语句时，`@var` 会被 `1` 替换。

  ```Plain
  SET @var = 1;
  SELECT @var, v1 from test;
  ```

- 避免重复计算。示例如下，执行 SELECT 语句时，`@var` 会被 `select sum(c1) from tbl` 命令的计算结果替换。

  ```Plain
  SET @var = (select sum(c1) from tbl);
  SELECT @var, v1 from test;
  ```

## 在查询中为自定义变量赋值

除 SET 语句外，StarRocks 还支持 MySQL 的赋值运算符 `:=`。`SET @var_name := expr` 等价于 `SET @var_name = expr`。在查询的 SELECT 列表中，`@var_name := expr` 将 `expr` 的值赋给变量，并返回该值。

### 不带 FROM 子句的查询

赋值的效果与 SET 语句相同。赋值在查询执行完成后生效，因此 EXPLAIN 和 PREPARE 不会给变量赋值。`expr` 必须是 StarRocks 在分析语句时能够折叠为常量的表达式，例如字面量或 `@v1 + 1`。如需将子查询或不能折叠的函数的结果赋给变量，请使用 SET。

```SQL
SELECT @v1 := 1, @v2 := @v1 + 1;
SELECT @v1, @v2;
```

### 带 FROM 子句的查询

SELECT 列表对每一行从左到右计算一次，变量的值会从一行保留到下一行。MySQL 查询（包括 Looker 等 BI 工具生成的查询）利用这一点为行编号、在分组内排名、计算累计值，或将当前行与上一行进行比较。变量通常在 FROM 子句的子查询中初始化。

示例一：为表中的行编号。

```SQL
SELECT @rn := @rn + 1 AS rn, t.*
FROM t, (SELECT @rn := 0) init
ORDER BY t.c1;
```

示例二：为每个分组内的行编号。

```SQL
SELECT IF(@dept = dept, @n := @n + 1, @n := 1) AS n, @dept := dept AS dept, salary
FROM employees, (SELECT @n := 0, @dept := '') init
ORDER BY dept, salary DESC;
```

示例三：计算累计值。

```SQL
SELECT @total := @total + amount AS running_total, orders.*
FROM orders, (SELECT @total := 0) init
ORDER BY order_time;
```

示例四：返回上一行的值。

```SQL
SELECT @prev AS prev_amount, @prev := amount AS amount
FROM (SELECT order_time, amount FROM orders ORDER BY order_time) o, (SELECT @prev := NULL) init;
```

StarRocks 并不逐行执行这类查询，而是根据赋值推导出返回相同结果的窗口函数。因此有如下规则：

- 按照查询的 ORDER BY 子句的顺序对各行求值。如果查询没有 ORDER BY 子句，或者该子句按读取或赋值了这些变量的 SELECT 项排序，则按照 FROM 子句中子查询的 ORDER BY 子句的顺序求值，如示例四。该子查询必须返回它用于排序的列。如果两者都不适用，则求值顺序不确定。对于关联多张表的查询，MySQL 可能在排序之前就对变量赋值。如需两者返回相同的结果，请像示例四那样在子查询中排序。
- 变量的新值要么不依赖于它的旧值，要么是在旧值上加上或减去某个值。两者都可以取决于某个条件（CASE、IF、IFNULL、COALESCE），但该条件不能读取这个变量本身。例如，支持 `@v := @v + c1`、`@v := IF(c1 > 0, c2, @v)` 和 `@v := IFNULL(c1, @v)`，而 `@v := @v * 2` 会返回错误。
- `:=` 只能用于 SELECT 列表。对每一行为变量赋值的查询不能在同一个 SELECT 中使用 DISTINCT、GROUP BY、HAVING、聚合函数、窗口函数或 OFFSET，其 WHERE 和 ORDER BY 子句也不能引用这些变量。如需按这样的 SELECT 项过滤或排序，请为其指定别名，并将查询放入子查询中，或者按别名排序。
- 在查询中，每个变量只有一种数据类型，由它的初始值和赋给它的值推导得出。整数值以 BIGINT 类型存储，日期值以字符串存储。
- 赋值只在包含它的 SELECT 列表中生效。外层查询、UNION 的其他分支以及后续语句看到的仍然是变量之前的值。
