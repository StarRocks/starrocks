# UPDATE

Modifies rows in a Primary Key table. In versions earlier than version 3.0, the UPDATE statement only supports simple syntax, such as `UPDATE <table_name> SET <column_name>=<expression> WHERE <where_condition>`. Starting from version 3.0, StarRocks enriches the syntax to support multi-table joins and common table expressions (CTEs). If you need to join the table to be updated with other tables in the database, you can reference these other tables in the FROM clause or CTE.

## Usage notes

```SQL
[ WITH <with_query> [, ...] ]
UPDATE <table_name>
SET <column_name> = <expression> [, ...]
[ FROM <from_item> [, ...] ]
WHERE <where_condition>
```

## Parameters

`with_query`

One or more CTEs that can be referenced by name in an UPDATE statement. CTEs are temporary result sets that can improve the readability of complex statements.

`table_name`

The name of the table to be updated.

`column_name`

The name of the column to be updated. It cannot include the table name. For example, 'UPDATE t1 SET col = 1' is not valid.

`expression`

The expression that assigns new values to the column.

`from_item`

One or more other tables in the database. These tables can be joined with the table to be updated based on the condition specified in the WHERE clause. The values of the rows in the result set are used to update the values for the specified columns in the matched rows in the table to be updated. For example, if the FROM clause is `FROM t1 WHERE t0.pk = t1.pk`, StarRocks converts the table expression in the FROM clause to `t0 JOIN t1 ON t0.pk=t1.pk` when executing the UPDATE statement.

`where_condition`

The condition based on which you want to update rows. Only rows that meet the WHERE condition can be updated. This parameter is required, because it helps prevent you from accidentally updating the entire table. If you want to update the entire table, you can use 'WHERE true'.

## Examples

For example, there are two tables `employees` and `accounts` in StarRocks. The table `employees` records employee information, and the table `accounts` records account information.

```SQL
CREATE TABLE employees
(
    id BIGINT NOT NULL,
    sales_count INT NOT NULL
) 
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO employees VALUES (1,100),(2,1000);

CREATE TABLE accounts 
(
    accounts_id BIGINT NOT NULL,
    name VARCHAR(26) NOT NULL,
    sales_person INT NOT NULL
) 
PRIMARY KEY (accounts_id)
DISTRIBUTED BY HASH(accounts_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO accounts VALUES (1,'Acme Corporation',2),(2,'Acme Corporation',3),(3,'Corporation',3);
```

If you need to increase the sales count of salespersons that manage Acme Corporation's account in the `employees` table by 1, you can execute the following statement:

```SQL
UPDATE employees
SET sales_count = sales_count + 1
FROM accounts
WHERE accounts.name = 'Acme Corporation'
   AND employees.id = accounts.sales_person;
```

You can also use a CTE to rewrite the above statement to improve readability.

```SQL
WITH acme_accounts as (
    SELECT * from accounts
     WHERE accounts.name = 'Acme Corporation'
)
UPDATE employees SET sales_count = sales_count + 1
FROM acme_accounts
WHERE employees.id = acme_accounts.sales_person;
```
