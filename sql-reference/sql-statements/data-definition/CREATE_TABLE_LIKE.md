# CREATE TABLE LIKE

## description

该语句用于创建一个表结构和另一张表完全相同的空表。

语法：

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name
```

说明:

1. 复制的表结构包括Column Defination、Partitions、Table Properties等
2. 用户需要对复制的原表有`SELECT`权限
3. 支持复制MySQL等外表

## Example

1. 在test1库下创建一张表结构和table1相同的空表，表名为table2

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1
    ```

2. 在test2库下创建一张表结构和test1.table1相同的空表，表名为table2

    ```sql
    CREATE TABLE test2.table2 LIKE test1.table1
    ```

3. 在test1库下创建一张表结构和MySQL外表table1相同的空表，表名为table2

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1
    ```

## keyword

CREATE,TABLE,LIKE
