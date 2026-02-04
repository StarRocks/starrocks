---
displayed_sidebar: docs
---

# string_agg

## 功能

将分组中的多个非 NULL 值连接成一个字符串。该函数为 [group_concat](./group_concat.md) 函数的一种语法糖形式，用于兼容 PostgreSQL 及 SQL Server 的同名函数，其底层实现完全复用现有的 [group_concat](./group_concat.md) 函数的逻辑。

## 语法

```SQL
VARCHAR STRING_AGG([DISTINCT] expr, delimiter
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]])
```

## 参数说明

- `expr`: 待拼接的值，支持的数据类型为 VARCHAR。可以指定 DISTINCT 关键字在连接之前移除分组中的重复值。如果想直接连接多个值，可以使用 [concat](../string-functions/concat.md) 和 [concat_ws](../string-functions/concat_ws.md) 来指定连接的方式。
- `delimiter`：字符串之间的分隔符，**必需参数**。该参数必须是 VARCHAR 类型。如果要使用空字符来连接，可以使用 `''`。
- ORDER BY 后可以跟 unsigned_integer (从 1 开始)、列名、或普通表达式。ORDER BY 用于指定按升序或降序对要连接的值进行排序。默认按升序排序。如果要按降序排序，需要指定 DESC。

> **NOTE**
>
> - STRING_AGG 是 PostgreSQL 及 SQL Server 兼容的函数，分隔符作为第二个参数，为必填参数。
> - 如果需要 MySQL 风格的语法（使用 `SEPARATOR` 关键字），请使用 [group_concat](./group_concat.md)。

## 返回值说明

返回值的数据类型为 VARCHAR。如果没有非 NULL 值，则返回 NULL。

您可以使用系统变量 [group_concat_max_len](../../System_variable.md#group_concat_max_len) 来控制可以返回的最大字符长度。默认值：1024。最小值：4。单位：字符。

变量的设置方法：

```sql
SET [GLOBAL | SESSION] group_concat_max_len = <value>
```

## 示例

1. 建表并插入数据。

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

2. 使用 string_agg 对值进行拼接。

   示例一：对每个部门的 `name` 列的值进行拼接，使用逗号作为分隔符，忽略 NULL 值。

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

   示例二：对每个部门的 `name` 列的值进行拼接，使用 ` | ` 作为分隔符。

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

   示例三：对每个部门的 `name` 列的值进行拼接，使用逗号作为分隔符，并使用 DISTINCT 对数据进行去重。

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

   示例四：按照 `salary` 的升序对员工姓名进行连接。

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

   示例五：按照 `salary` 的降序对员工姓名进行连接。

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

   示例六：使用 concat 函数嵌套，以 "name:salary" 的格式连接。

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

   示例七：对所有员工姓名进行全局聚合（不分组）。

   ```sql
   select string_agg(name, ', ' order by name) as all_names 
   from employees;
   +---------------------------+
   | all_names                 |
   +---------------------------+
   | Alice,Bob,Charlie,David,Eve |
   +---------------------------+
   ```

   示例八：没有符合条件的结果，返回 NULL。

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

   示例九：将返回字符串的最大长度限制在 10 个字符。

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

## 与 GROUP_CONCAT 的对比

STRING_AGG 和 GROUP_CONCAT 提供相同的功能，但语法不同：

```sql
-- PostgreSQL 风格 (STRING_AGG)
SELECT STRING_AGG(name, ',' ORDER BY name) FROM employees;

-- MySQL 风格 (GROUP_CONCAT)
SELECT GROUP_CONCAT(name ORDER BY name SEPARATOR ',') FROM employees;
```

两者产生相同的结果。选择使用哪个取决于您的 SQL 方言偏好或应用程序的兼容性需求。

## keywords

STRING_AGG, GROUP_CONCAT, CONCAT, ARRAY_AGG, POSTGRESQL
