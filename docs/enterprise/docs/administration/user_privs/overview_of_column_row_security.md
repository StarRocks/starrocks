# Column and row level security

This topic describes what a row access policy is, how to create and apply a row access policy, two use cases in typical scenarios, how to manage row access policies, and the limits when you work with a row access policy.

For the overview of column and row-level security, see [Understand column and row-level security](https://starrocks.feishu.cn/docx/ZB4ad5UePoNGVZxp6GDcOzBEn0g).

For the privileges required for each SQL operation, see [Manage privileges for policies](https://starrocks.feishu.cn/docx/N0k9dIlxKojsSPxzM1WcuiWRn2M).

## Definition

Row-level security allows you to apply a row access policy to a table or view to determine which rows are visible in the query result.

You can include conditions and functions in the policy expression of a row access policy to transform the data at query runtime when the conditions are met.

A row access policy can be added to a table or view either when the table is created or after the table is created. 

## Create a row access policy

Creates a row access policy in the current database. A policy consists of column name, [data type](https://docs.snowflake.com/en/sql-reference/data-types.html), conditions, and functions.

**Syntax****:**

```Haskell
CREATE ROW ACCESS POLICY [ IF NOT EXISTS ] <name> 
AS ( <arg_name> <arg_type> [ , ... ] ) 
RETURNS boolean ->
<expression_on_arg_name>
[ COMMENT = '<string_literal>' ]
```

| **Parameter**            | **Required** | **Description**                                              |
| ------------------------ | ------------ | ------------------------------------------------------------ |
| `name`                   | Yes          | The name of the policy, which must be unique across the database. Policies can be referenced across databases and catalogs in the format `catalog.db.policy`. If no catalog is specified, the current catalog is used. |
| `arg_name`               | Yes          | The name of the column to mask.                              |
| `arg_type`               | Yes          | The data type of the column to mask.                         |
| RETURNS                  | Yes          | The return data type must be BOOLEAN.                        |
| `expression_on_arg_name` | Yes          | The expression that is used as the filter condition, which can be any conditional function, such as if()，case when()，and ifnull(). |
| `COMMENT`                | No           | The description of the policy.                               |

**Examples:**

Example 1: Create a row access policy, which only allows `sales_asia` to see data in the `asia` region, `sales_uk` to see data in the `uk` region, and `ACCOUNTADMIN` to see all the data.

```Haskell
CREATE ROW ACCESS POLICY region_data AS
(region varchar(50)) RETURNS boolean ->
CASE WHEN is_role_in_session('sales_asia')=1 and region = 'asia' THEN true
     WHEN is_role_in_session('sales_uk')=1 and region = 'uk' THEN true
 ELSE false
 END;
```

Example 2: Nest a subquery in a row access policy, which allows the current role to see only data in its own region.

```Haskell
CREATE ROW ACCESS POLICY rap_sales_manager_regions_2 AS
(sales_region varchar) RETURNS boolean
 ->
 CASE WHEN EXISTS (
            select * from map
              where 'role' = current_role()
                and 'sales_region' = region
          ) THEN true 
  ELSE false
  END;
```

## Apply a row access policy

After a policy is created, you can apply it to an existing table.

**Syntax****:** 

```Haskell
ALTER TABLE <tbl_name> ADD ROW ACCESS POLICY <name> ON (<cond_col1>[, <cond_col2>, ...])
```

**Example:** 

```Haskell
ALTER TABLE `sales_info` ADD ROW ACCESS POLICY region_data ON (region);
```

You can also apply an existing row access policy to a table using the WITH clause when you create a table.  

**Syntax****:** 

```Haskell
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition2, ...]])
[ENGINE = [olap|mysql|elasticsearch|hive|iceberg|hudi|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
distribution_desc
WITH ROW ACCESS POLICY <name> ON (<cond_col1 [, <cond_col2> , ...])  
[WITH ROW ACCESS POLICY <name> ON (<cond_col12> [, <cond_col1> , ...]) ...] 
[rollup_index]
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

**Examples:**

```Haskell
CREATE TABLE `sales_info` (
    name varchar(50),
    phone string,
    region varchar(50),
    sales INT)
 WITH ROW ACCESS POLICY region_data ON (region);
```

## Use case - Filter data by region

Users have sales data in three regions and want sales staff to view only data in their own region.

1. Create a user information table `sales_info` and insert data.

```Haskell
CREATE TABLE `sales_info` (
    name varchar(50),
    phone string,
    region varchar(50),
    sales INT);

INSERT INTO `sales_info` VALUES
('lily','886410','asia',11),
('richard','654321','uk',16),
('amber','789165','africa',17);
```

1. Create two different roles.

```Haskell
CREATE ROLE `sales_asia`,`sales_uk`;
```

1. Grant the roles the privilege to query data from the table and assign the roles to the current user.

```Haskell
GRANT SELECT ON TABLE `sales_info` TO ROLE `sales_asia`;
GRANT SELECT ON TABLE `sales_info` TO ROLE `sales_uk`;

GRANT `sales_asia`,`sales_uk` TO USER '<current_user>';
```

1. Create a row access policy which uses CASE WHEN as the filter condition. This policy allows different roles to view only data of their own region.

```Haskell
CREATE ROW ACCESS POLICY region_data AS
(region varchar(50)) RETURNS boolean ->
CASE WHEN current_role() ='sales_asia' and region = 'asia' THEN true
     WHEN current_role() ='sales_uk' and region = 'uk' THEN true
 ELSE false
 END;
```

1. Apply the policy to the table.

```Haskell
ALTER TABLE `sales_info` ADD ROW ACCESS POLICY region_data ON (region);
```

1. Use roles `sales_asia` and `sales_uk` to query data. The results show that the `sales_asia` role can only see data in the `asia` region and the `sales_uk` role can only see data in the `uk` region.

```Haskell
SET ROLE `sales_asia`;
SELECT * FROM `sales_info`;
+------+--------+--------+-------+
| name | phone  | region | sales |
+------+--------+--------+-------+
| lily | 886410 | asia   |    11 |
+------+--------+--------+-------+

SET ROLE `sales_uk`;
SELECT * FROM `sales_info`;
+---------+--------+--------+-------+
| name    | phone  | region | sales |
+---------+--------+--------+-------+
| richard | 654321 | uk     |    16 |
+---------+--------+--------+-------+
```

## Use case - Use a mapping table for data lookup

You can customize a mapping table based on a business table to map dimensions from the business table. Mapping table is not a special concept but a common StarRocks table. You can create a row access policy based on the mapping table and specify filter conditions to filter data from the business table. When data in the mapping table changes, the policy is automatically updated without requiring you to modify the policy.

Use a typical case as an example:

1. Create a business table `revenue` and insert data.

```Haskell
CREATE TABLE `revenue` (
    customer_id varchar(50),
    region varchar(50),
    discount float,
    revenue INT);

INSERT INTO `revenue` VALUES
('supermarket1','LA', 0.9,100),
('grocery_store3','NYC',0.8,150),
('whole_food2','NYC',0.9,120);
```

1. Create a mapping table `sales_manager_region` which stores the owner of each region. In the following steps, roles assigned to user `'Chelsea'@'%'` can only see data in the `LA` region.

```Haskell
CREATE TABLE sales_manager_region (
    name varchar(50),
    region varchar(80)
 );

INSERT INTO sales_manager_region VALUES
("'Chelsea'@'%'",'LA'),
("'Amber'@'%'",'NYC');
```

1. Create roles `sales_manager` and `sales`. Grant the SELECT privilege on tables `revenue` and `sales_manager_region` to the two roles. Assign `sales_manager` to the current user and assign `sales` to user `'Chelsea'@'%'`.

```Haskell
CREATE ROLE `sales_manager`,`sales`;

GRANT SELECT ON TABLE `revenue`,`sales_manager_region` TO ROLE `sales_manager`;
GRANT SELECT ON TABLE `revenue`,`sales_manager_region` TO ROLE `sales`;

GRANT `sales_manager` TO USER '<current_user>';

CREATE USER 'Chelsea'@'%';
GRANT `sales` TO USER 'Chelsea'@'%';
```

The required filtering effect

- `sales_manager` can view all the data in the `revenue`  table.
- Other roles can only view the data in its own region.
- When the region owner changes, no policy update is required.

1. Create a policy which contains a subquery against the mapping table `sales_manager_region`. This policy allows the `sales_manager` role to view all data, `sales` to view data only in its own region, and when the region owner changes, privileges can be updated without the need to modify the policy.

```Haskell
CREATE ROW ACCESS POLICY sales_policy AS (region_data varchar)
RETURNS boolean ->
     current_role() = 'sales_manager'
     OR current_role() = 'sales' and EXISTS (SELECT 1 FROM sales_manager_region WHERE 
             name = current_user() and region = region_data);
```

1. Apply the policy to table `revenue`.

```Haskell
ALTER TABLE `revenue` ADD ROW ACCESS POLICY sales_policy ON (region);
```

1. Switch to role `sales_manager` and query data from `revenue`. `sales_manager` can view all the data in the table.

```Haskell
SET ROLE sales_manager;
SELECT * FROM `revenue`;
+----------------+--------+----------+---------+
| customer       | region | discount | revenue |
+----------------+--------+----------+---------+
| supermarket1   | LA     |      0.9 |     100 |
| grocery_store3 | NYC    |      0.8 |     150 |
| whole_food2    | NYC    |      0.9 |     120 |
+----------------+--------+----------+---------+
```

1. Perform operations as user Chelsea and switch to the `sales` role. This role can only access the data row whose `region` is `LA`.

```Haskell
EXECUTE AS 'Chelsea'@'%' WITH NO REVERT;

SET ROLE sales;

SELECT * FROM `revenue`;
+----------------+--------+----------+---------+
| customer       | region | discount | revenue |
+----------------+--------+----------+---------+
| supermarket1   | LA     |      0.9 |     100 |
+----------------+--------+----------+---------+
```

## Manage row access policies

### Unset row access policies

Unsets one or all row access policies from a table.

**Syntax****:** 

```Haskell
ALTER TABLE <tbl_name> DROP ROW ACCESS POLICY <name>
ALTER TABLE <tbl_name> DROP ALL ROW ACCESS POLICIES
```

**Examples:**

```Haskell
ALTER TABLE sales_info DROP ROW ACCESS POLICY region_data;
ALTER TABLE sales_info DROP ALL ROW ACCESS POLICIES;
```

### Modify a row access policy

You can only modify the policy body, rename the policy, or update the comment of the policy. The new policy takes effect immediately after being created without requiring you to re-apply it to each table.

**Syntax****:** 

```Haskell
ALTER ROW ACCESS POLICY [ IF EXISTS ] <name> SET BODY -> <expression_on_arg_name>
ALTER ROW ACCESS POLICY [ IF EXISTS ] <name> RENAME TO <new_name>
ALTER ROW ACCESS POLICY [ IF EXISTS ] <name> SET COMMENT = '<string_literal>'
```

**Examples:**

```Haskell
ALTER ROW ACCESS POLICY region_data RENAME TO data_region;

ALTER ROW ACCESS POLICY region_data SET COMMENT = 'test';
```

### Query all row access policies

Queries all row access policies in a database.

```SQL
SHOW ROW ACCESS POLICIES;
+-----------------------------+------------+-----------------+----------+
| Name                        | Type       | Catalog         | Database |
+-----------------------------+------------+-----------------+----------+
| region_data                 | ROW ACCESS | default_catalog | zj_test  |
| rap_sales_manager_regions_2 | ROW ACCESS | default_catalog | zj_test  |
```

### Query the CREATE statement of a row access policy

**Syntax****:** 

```Haskell
SHOW CREATE ROW ACCESS POLICY <name>
```

**Examples:**

```Haskell
+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Policy      | Create Policy                                                                                                                                                                                                                                                                                                 |
+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| region_data | CREATE ROW ACCESS POLICY region_data AS (region varchar) RETURNS boolean -> CASE WHEN (((CURRENT_ROLE()) = 'ROLE1') AND (`region` = 'uk')) THEN TRUE WHEN (((CURRENT_ROLE()) = 'ROLE2') AND (`region` = 'us')) THEN TRUE WHEN ((CURRENT_ROLE()) = 'ACCOUNTADMIN') THEN TRUE ELSE FALSE END COMMENT "for test" |
+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Drop a row access policy

You are not allowed to drop a policy that has been applied to tables. If you want to drop such a policy, revoke it from all tables to which this policy has been applied and then drop this policy.

```SQL
DROP ROW ACCESS POLICY <name>
```

## Limits

- When you create a row access policy, the return type for the policy must be BOOLEAN.
- If a row access policy is applied to a base table, you cannot create a materialized view based on that table.
- Similarly, if a table is used as the base table of a materialized view, you cannot apply a row access policy to this table. 
- A column with a row access policy applied can still be used as a conditional column in another masking policy or be referenced in the subquery of another policy.

## See also

Policy creation and application are controlled by privileges such as CREATE, APPLY, ALTER, and DROP. For more information about how to grant these privileges, the privileges required by each command, and privilege management mode, see [Manage privileges for policies](https://starrocks.feishu.cn/docx/N0k9dIlxKojsSPxzM1WcuiWRn2M).