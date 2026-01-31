---
displayed_sidebar: docs
---

# string_agg

## Description

Concatenates non-null values from a group into a single string. This function is a syntactic sugar for the [group_concat](./group_concat.md) function, designed to be compatible with the PostgreSQL and SQL Server functions of the same name. Its underlying implementation fully reuses the logic of the existing [group_concat](./group_concat.md) function.

## Syntax

```SQL
VARCHAR STRING_AGG([DISTINCT] expr, delimiter
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]])
```

## Parameters

- `expr`: the values to concatenate, with null values ignored. It must evaluate to VARCHAR. You can optionally specify `DISTINCT` to eliminate duplicate values from the output string. If you want to concatenate multiple `expr` directly, use [concat](../string-functions/concat.md) or [concat_ws](../string-functions/concat_ws.md) to specify formats.
- `delimiter`: the **required** separator used to concatenate non-null values from different rows. It must be of VARCHAR type. To eliminate the separator, specify an empty string `''`.
- Items in ORDER BY can be unsigned integers (starting from 1), column names, or normal expressions. The results are sorted in ascending order by default. You can also explicitly specify the ASC keyword. If you want to sort results in descending order, add the DESC keyword to the name of the column you are sorting.

> **NOTE**
>
> - STRING_AGG is a PostgreSQL and SQL Server compatible function where the delimiter is specified as the second parameter and is required.
> - For MySQL-style syntax (using the `SEPARATOR` keyword), use [group_concat](./group_concat.md).

## Return value

Returns a string value for each group and returns NULL if there are no non-NULL values.

You can limit the length of the string returned by string_agg by setting the [session variable](../../System_variable.md) `group_concat_max_len`, which defaults to 1024. Minimum value: 4. Unit: characters.

Example:

```sql
SET [GLOBAL | SESSION] group_concat_max_len = <value>;
```

## Examples

1. Create a table `employees` which contains employee information.

   ```sql
   CREATE TABLE `employees` (
     `dept_id` int(11) NULL COMMENT "",
     `name` varchar(255) NULL COMMENT "",
     `salary` int(11) NULL COMMENT ""
   ) ENGINE=OLAP
   DUPLICATE KEY(`dept_id`)
   DISTRIBUTED BY HASH(`dept_id`) BUCKETS 4
   PROPERTIES (
   "replication_num" = "1"
   );

   insert into employees values (1, "Alice", 5000);
   insert into employees values (1, "Bob", 6000);
   insert into employees values (2, "Charlie", 5500);
   insert into employees values (2, "David", 7000);
   insert into employees values (2, NULL, 8000);
   insert into employees values (3, "Eve", NULL);

   select * from employees order by dept_id, name;
   +---------+---------+--------+
   | dept_id | name    | salary |
   +---------+---------+--------+
   |       1 | Alice   |   5000 |
   |       1 | Bob     |   6000 |
   |       2 | Charlie |   5500 |
   |       2 | David   |   7000 |
   |       2 | NULL    |   8000 |
   |       3 | Eve     |   NULL |
   +---------+---------+--------+
   ```

2. Use string_agg to concatenate values.

   Example 1: Concatenate employee names in each department with comma as delimiter, ignoring NULL values.

   ```sql
   select dept_id, string_agg(name, ',') as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Alice,Bob     |
   |       2 | Charlie,David |
   |       3 | Eve           |
   +---------+---------------+
   ```

   Example 2: Concatenate employee names using ` | ` as delimiter.

   ```sql
   select dept_id, string_agg(name, ' | ') as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+-----------------+
   | dept_id | names           |
   +---------+-----------------+
   |       1 | Alice | Bob     |
   |       2 | Charlie | David |
   |       3 | Eve             |
   +---------+-----------------+
   ```

   Example 3: Concatenate distinct employee names using comma as delimiter.

   ```sql
   insert into employees values (1, "Alice", 5500);
   
   select dept_id, string_agg(distinct name, ',') as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Alice,Bob     |
   |       2 | Charlie,David |
   |       3 | Eve           |
   +---------+---------------+
   ```

   Example 4: Concatenate employee names sorted by salary in ascending order.

   ```sql
   select dept_id, string_agg(name, ',' order by salary) as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Alice,Bob     |
   |       2 | Charlie,David |
   |       3 | Eve           |
   +---------+---------------+
   ```

   Example 5: Concatenate employee names sorted by salary in descending order.

   ```sql
   select dept_id, string_agg(name, ',' order by salary desc) as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Bob,Alice     |
   |       2 | David,Charlie |
   |       3 | Eve           |
   +---------+---------------+
   ```

   Example 6: Concatenate employee information in "name:salary" format using nested concat().

   ```sql
   select dept_id, 
          string_agg(concat(name, ':', cast(salary as varchar)), '; ' order by salary) as employee_info
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+-----------------------------+
   | dept_id | employee_info               |
   +---------+-----------------------------+
   |       1 | Alice:5000; Bob:6000        |
   |       2 | Charlie:5500; David:7000    |
   |       3 | Eve:NULL                    |
   +---------+-----------------------------+
   ```

   Example 7: Concatenate all employee names globally (without GROUP BY).

   ```sql
   select string_agg(name, ', ' order by name) as all_names 
   from employees;
   +---------------------------+
   | all_names                 |
   +---------------------------+
   | Alice,Bob,Charlie,David,Eve |
   +---------------------------+
   ```

   Example 8: No matching result is found and NULL is returned.

   ```sql
   select string_agg(name, ',') as names 
   from employees 
   where dept_id > 100;
   +-------+
   | names |
   +-------+
   | NULL  |
   +-------+
   ```

   Example 9: Limit the length of the returned string to 10 characters.

   ```sql
   set group_concat_max_len = 10;

   select dept_id, string_agg(name, ',' order by name) as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+------------+
   | dept_id | names      |
   +---------+------------+
   |       1 | Alice,Bob  |
   |       2 | Charlie,Da |
   |       3 | Eve        |
   +---------+------------+
   ```

## Comparison with GROUP_CONCAT

STRING_AGG and GROUP_CONCAT provide the same functionality but with different syntax:

```sql
-- PostgreSQL style (STRING_AGG)
SELECT STRING_AGG(name, ',' ORDER BY name) FROM employees;

-- MySQL style (GROUP_CONCAT)
SELECT GROUP_CONCAT(name ORDER BY name SEPARATOR ',') FROM employees;
```

Both produce identical results. Choose which to use based on your SQL dialect preference or application compatibility requirements.

## keyword

STRING_AGG, GROUP_CONCAT, CONCAT, ARRAY_AGG, POSTGRESQL
