# UPDATE

Updates rows in a Primary Key table.

StarRocks supports the UPDATE statement since v2.3, which only supports single-table UPDATE and does not support common table expressions (CTEs). Starting from version 3.0, StarRocks enriches the syntax to support multi-table joins and CTEs. If you need to join the table to be updated with other tables in the database, you can reference these other tables in the FROM clause or CTE. Since version 3.1, the UPDATE statement supports the partial updates in column mode, which is suitable for scenarios involving a small number of columns but a large number of rows, resulting in faster update speeds.

This command requires the UPDATE privilege on the table you want to update.

## Usage notes

When executing the UPDATE statement involving multiple tables, StarRocks converts the table expression in the FROM clause of the UPDATE statement into an equivalent JOIN query statement. Therefore, make sure that the table expression that you specify in the FROM clause of the UPDATE statement supports this conversion. For example, the UPDATE statement is 'UPDATE t0 SET v1=t1.v1 FROM t1 WHERE t0.pk = t1.pk;'. The table expression in the FROM clause can be converted to 't0 JOIN t1 ON t0.pk=t1.pk;'. StarRocks matches the data rows to be updated based on the result set of the JOIN query. It is possible that multiple rows in the result set match a certain row in the table to be updated. In this scenario, that row is updated based on the value of a random row among these multiple rows.

## Syntax

### Single-table UPDATE

If the data rows of the table to be updated meet the WHERE condition, the specified columns of these data rows are assigned new values.

```SQL
[ WITH <with_query> [, ...] ]
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    WHERE <where_condition>
```

### Multi-table UPDATE

The result set from the multi-table join is matched against the table to be updated. If the data rows of the table to be updated match the result set and meet the WHERE condition, the specified columns of these data rows are assigned new values.

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

The condition based on which you want to update rows. Only rows that meet the WHERE condition can be updated. This parameter is required, because it helps prevent you from accidentally updating the entire table. If you want to update the entire table, you can use 'WHERE true'. However, this parameter is not required for [partial updates in column mode](#partial-updates-in-column-mode-since-v31).

## Partial updates in column mode (since v3.1)

Partial updates in column mode are suitable for scenarios where only a small number of columns, but a large number of rows need to be updated. In such scenarios, enabling the column mode offers faster update speeds. For example, in a table with 100 columns, if only 10 columns (10% of the total) are updated for all rows, the update speed of the column mode is 10 times faster.

The system variable `partial_update_mode` controls the mode of partial updates and supports the following values:

- `auto` (default): The system automatically determines the mode of partial updates by analyzing the UPDATE statement and the columns involved. If the following criteria are met, the system automatically uses the column mode:
  - The percentage of updated columns compared to the total number of columns is less than 30%, and the number of updated columns is fewer than 4.
  - The update statement does not use a WHERE condition.
Otherwise, the system does not use the column mode.

- `column`: The column mode is used for the partial updates, which is particularly suitable for the partial updates which involve a small number of columns and a large number of rows.

You can use `EXPLAIN UPDATE xxx` to view the mode of partial updates.

## Examples

### Single-table UPDATE

Create a table `Employees` to record employee information and insert five data rows into the table.

```SQL
CREATE TABLE Employees (
    EmployeeID INT,
    Name VARCHAR(50),
    Salary DECIMAL(10, 2)
)
PRIMARY KEY (EmployeeID) 
DISTRIBUTED BY HASH (EmployeeID)
PROPERTIES ("replication_num" = "3");

INSERT INTO Employees VALUES
    (1, 'John Doe', 5000),
    (2, 'Jane Smith', 6000),
    (3, 'Robert Johnson', 5500),
    (4, 'Emily Williams', 4500),
    (5, 'Michael Brown', 7000);
```

If you need to give a 10% raise to all employees, you can execute the following statement:

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- Increase the salary by 10%.
WHERE true;
```

If you need to give a 10% raise to employees with salaries lower than the average salary, you can execute the following statement:

```SQL
UPDATE Employees
SET Salary = Salary * 1.1   -- Increase the salary by 10%.
WHERE Salary < (SELECT AVG(Salary) FROM Employees);
```

You can also use a CTE to rewrite the above statement to improve readability.

```SQL
WITH AvgSalary AS (
    SELECT AVG(Salary) AS AverageSalary
    FROM Employees
)
UPDATE Employees
SET Salary = Salary * 1.1   -- Increase the salary by 10%.
FROM AvgSalary
WHERE Employees.Salary < AvgSalary.AverageSalary;
```

### Multi-table UPDATE

Create a table `Accounts` to record account information and insert three data rows into the table.

```SQL
CREATE TABLE Accounts (
    Accounts_id BIGINT NOT NULL,
    Name VARCHAR(26) NOT NULL,
    Sales_person VARCHAR(50) NOT NULL
) 
PRIMARY KEY (Accounts_id)
DISTRIBUTED BY HASH (Accounts_id)
PROPERTIES ("replication_num" = "3");

INSERT INTO Accounts VALUES
    (1,'Acme Corporation','John Doe'),
    (2,'Acme Corporation','Robert Johnson'),
    (3,'Acme Corporation','Lily Swift');
```

If you need to give a 10% raise to employees in the table `Employees` who manage accounts for Acme Corporation, you can execute the following statement:

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- Increase the salary by 10%.
FROM Accounts
WHERE Accounts.name = 'Acme Corporation'
    AND Employees.Name = Accounts.Sales_person;
```

You can also use a CTE to rewrite the above statement to improve readability.

```SQL
WITH Acme_Accounts as (
    SELECT * from Accounts
    WHERE Accounts.name = 'Acme Corporation'
)
UPDATE Employees SET Salary = Salary * 1.1 -- Increase the salary by 10%.
FROM Acme_Accounts
WHERE Employees.Name = Acme_Accounts.Sales_person;
```
