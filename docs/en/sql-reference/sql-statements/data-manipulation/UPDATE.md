---
displayed_sidebar: "English"
---

# UPDATE

Updates rows in a Primary Key table.

StarRocks supports the UPDATE statement since v2.3, which only supports single-table UPDATE and does not support common table expressions (CTEs).

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

From v2.3 to v2.5, the UPDATE statement only supports single-table UPDATE and does not support common table expressions (CTEs).
