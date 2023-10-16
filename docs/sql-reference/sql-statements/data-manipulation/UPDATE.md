# UPDATE

Updates rows in a Primary Key table.

<<<<<<< HEAD
=======
StarRocks supports the UPDATE statement since v2.3, which only supports single-table UPDATE and does not support common table expressions (CTEs). Starting from version 3.0, StarRocks enriches the syntax to support multi-table joins and CTEs. If you need to join the table to be updated with other tables in the database, you can reference these other tables in the FROM clause or CTE. Since version 3.1, the UPDATE statement supports the partial updates in column mode, which is suitable for scenarios involving a small number of columns but a large number of rows, resulting in faster update speeds.

This command requires the UPDATE privilege on the table you want to update.

## Usage notes

When executing the UPDATE statement involving multiple tables, StarRocks converts the table expression in the FROM clause of the UPDATE statement into an equivalent JOIN query statement. Therefore, make sure that the table expression that you specify in the FROM clause of the UPDATE statement supports this conversion. For example, the UPDATE statement is 'UPDATE t0 SET v1=t1.v1 FROM t1 WHERE t0.pk = t1.pk;'. The table expression in the FROM clause can be converted to 't0 JOIN t1 ON t0.pk=t1.pk;'. StarRocks matches the data rows to be updated based on the result set of the JOIN query. It is possible that multiple rows in the result set match a certain row in the table to be updated. In this scenario, that row is updated based on the value of a random row among these multiple rows.

>>>>>>> d9877f2d44 ([Doc] add precise version info for update syntax (#32854))
## Syntax

```SQL
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    WHERE <where_condition>
```

## Parameters

`table_name`

The name of the table to be updated.

`column_name`

The name of the column to be updated. It cannot include the table name. For example, 'UPDATE t1 SET col = 1' is not valid.

`expression`

The expression that assigns new values to the column.

`where_condition`

The condition based on which you want to update rows. Only rows that meet the WHERE condition can be updated. This parameter is required, because it helps prevent you from accidentally updating the entire table. If you want to update the entire table, you can use 'WHERE true'.

## Examples

Create a table `Employees` to record employee information and insert five data rows into the table.

```SQL
CREATE TABLE Employees (
    EmployeeID INT,
    Name VARCHAR(50),
    Salary DECIMAL(10, 2)
)
PRIMARY KEY (EmployeeID) 
DISTRIBUTED BY HASH (EmployeeID) BUCKETS 1
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

## Limitations

In V2.5, the UPDATE statement only supports single-table UPDATE and does not support common table expressions (CTEs).
