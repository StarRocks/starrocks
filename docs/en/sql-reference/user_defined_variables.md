---
displayed_sidebar: docs
description: "StarRocks 2.5 and later versions support user-defined variables."
products: [oss, enterprise, cloud, anywhere]
---

# User-defined variables

This topic describes how to declare and use user-defined variables.

StarRocks 2.5 and later versions support user-defined variables. These variables are used to store specific values that are referenced in later SQL statements, thereby simplifying the writing of SQL statements and avoiding duplicate computation.

## Usage notes

- User-defined variables are variables, which can be created by the user and exist in the session. This means that no one can access user-defined variables that have been declared by another user, and when the session is closed these variables expire.
- StarRocks does not support using the SHOW statement to display existing user-defined variables.
- The following types of values cannot be declared as user-defined variables: BITMAP, HLL, and PERCENTILE. User-defined variables of the JSON type are converted to the STRING type for storage.
- From v3.2.7 onwards, StarRocks supports the ARRAY type as a user-defined variable.

## Declare user-defined variables

### Syntax

```Plain
SET @var_name = expr [, ...];
```

> **NOTE**
>
> - All variables must be preceded by a single at sign (@).
> - Multiple variables can be declared in the same SET statement and need to be separated with commas (`,`).
> - You can declare the same variable multiple times. The newly declared value overwrites the original value.
> - If an undeclared variable is used, the value of the variable is `NULL` by default, and the NULL type is STRING.

### Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| var_name      | Yes          | The name of the user-defined variable. The naming conventions are as follows:<ul><li>The name can contain letters, digits (0-9), and underscores (`_`). </li><li>The name cannot exceed 64 characters in length.</li></ul>You can declare a string as a user-defined variable, such as @'my-var', @"my-var", and @\`my-var\`. User-defined variables of the STRING type can contain characters other than letters, digits, and underscores (_), such as periods (.). |
| expr          | Yes          | The value of the user-defined variable. You can specify a number (such as 43) or a complex expression (such as the value returned by a SELECT statement) for this parameter. The data type of the variable is the same as the data type of the result returned by the expression. |

### Examples

Example 1: Declare a number as a user-defined variable.

```SQL
SET @var = 43;
```

Example 2: Declare the value returned by a SELECT query as a user-defined variable.

```SQL
SET @var = (SELECT SUM(v1) FROM test);
```

Example 3: Declare multiple user-defined variables in the same SET statement.

```SQL
SET @v1=1, @v2=2;
```

## Use user-defined variables in SQL

- Simplifies the writing of SQL statements. For example, when you execute the following SELECT statement, StarRocks parses `@var` as `1`.

  ```Plain
  SET @var = 1;
  SELECT @var, v1 from test;
  ```

- Avoids duplicate computation. For example, when you execute the following SELECT statement, StarRocks parses `@var` as the result returned by the `select sum(c1) from tbl` command.

  ```Plain
  SET @var = (select sum(c1) from tbl);
  SELECT @var, v1 from test;
  ```

## Assign user-defined variables in a query

In addition to the SET statement, StarRocks supports the MySQL assignment operator `:=`. `SET @var_name := expr` is equivalent to `SET @var_name = expr`. In the select list of a query, `@var_name := expr` assigns the value of `expr` to the variable and returns that value.

### Queries without a FROM clause

The assignment has the same effect as a SET statement. `expr` must be an expression that StarRocks can fold into a constant when it analyzes the statement, such as a literal or `@v1 + 1`. To assign the result of a subquery or of a function that is not folded, use SET.

```SQL
SELECT @v1 := 1, @v2 := @v1 + 1;
SELECT @v1, @v2;
```

### Queries with a FROM clause

The select list is evaluated once for each row, from left to right, and a variable keeps its value from one row to the next. MySQL queries, including the ones that BI tools such as Looker generate, use this to number rows, to rank rows within groups, to compute running totals, or to compare a row with the previous one. The variables are usually initialized in a subquery in the FROM clause.

Example 1: Number the rows of a table.

```SQL
SELECT @rn := @rn + 1 AS rn, t.*
FROM t, (SELECT @rn := 0) init
ORDER BY t.c1;
```

Example 2: Number the rows of each group.

```SQL
SELECT IF(@dept = dept, @n := @n + 1, @n := 1) AS n, @dept := dept AS dept, salary
FROM employees, (SELECT @n := 0, @dept := '') init
ORDER BY dept, salary DESC;
```

Example 3: Compute a running total.

```SQL
SELECT @total := @total + amount AS running_total, orders.*
FROM orders, (SELECT @total := 0) init
ORDER BY order_time;
```

Example 4: Return the value of the previous row.

```SQL
SELECT @prev AS prev_amount, @prev := amount AS amount
FROM (SELECT order_time, amount FROM orders ORDER BY order_time) o, (SELECT @prev := NULL) init;
```

StarRocks does not evaluate such a query row by row. It derives window functions from the assignments, which return the same result. This leads to the following rules:

- The rows are evaluated in the order of the ORDER BY clause of the query. If the query has no ORDER BY clause, or if the clause sorts by a select item that reads or assigns one of the variables, the rows are evaluated in the order of the ORDER BY clause of the subquery in the FROM clause, as in Example 4. The subquery must return the columns that it sorts by. If neither applies, the order is undefined. For a query that joins tables, MySQL may assign the variables before it sorts the rows. Sort in a subquery, as in Example 4, to get the same result from both.
- The new value of a variable must either not depend on its old value, or be the old value with something added to or subtracted from it. Both can depend on a condition (CASE, IF, IFNULL, COALESCE) that does not read the variable itself. For example, `@v := @v + c1`, `@v := IF(c1 > 0, c2, @v)`, and `@v := IFNULL(c1, @v)` are supported, whereas `@v := @v * 2` returns an error.
- `:=` can be used only in the select list. A query that assigns variables for each row cannot use DISTINCT, GROUP BY, aggregate functions, window functions, or OFFSET in the same SELECT, and its WHERE and ORDER BY clauses cannot reference these variables. To filter or sort by such a select item, give it an alias and put the query into a subquery, or sort by the alias.
- Within the query, each variable has a single data type, which is derived from its initial value and from the values assigned to it. Integer values are stored as BIGINT, and date values as strings.
- The assignments take effect only within the select list that contains them. An outer query, another branch of a UNION, and the statements that follow still see the value that the variable had before.
