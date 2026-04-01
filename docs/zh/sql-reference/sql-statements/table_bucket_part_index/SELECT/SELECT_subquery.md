---
displayed_sidebar: docs
sidebar_label: "Subquery"
---

# 子查询

根据相关性，子查询分为以下两种类型：

- 非相关子查询：独立于外部查询获得结果。
- 相关子查询：需要来自外部查询的值。

## 非相关子查询

非相关子查询支持 [NOT] IN 和 EXISTS。

**示例**

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

从 v3.0 版本开始，您可以在 `SELECT... FROM... WHERE... [NOT] IN` 的 WHERE 子句中指定多个字段，例如，第二个 SELECT 语句中的 `WHERE (x,y)`。

## 相关子查询

相关子查询支持 [NOT] IN 和 [NOT] EXISTS。

**示例**

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

子查询也支持标量量化查询。它可以分为不相关标量量化查询、相关标量量化查询和作为通用函数参数的标量量化查询。

## 示例

1. 具有 = 符号的非相关标量量化查询。例如，输出工资最高的人的信息。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. 具有谓词 `>`, `<` 等的不相关标量量化查询。例如，输出关于工资高于平均水平的人员的信息。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 相关的标量量子查询。例如，输出每个部门的最高工资信息。

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. 标量量子查询用作普通函数的参数。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```
