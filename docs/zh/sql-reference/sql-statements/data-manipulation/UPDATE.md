---
displayed_sidebar: "Chinese"
---

# UPDATE

该语句用于更新一张主键模型表中的数据行。

2.3 ～ 2.5 版本，StarRocks 提供 UPDATE 语句，并且仅支持单表 UPDATE 且不支持公用表表达式（CTE）。

## 语法

```SQL
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    WHERE <where_condition>
```

## 参数说明

`table_name`

待更新的表的名称。

`column_name`

待更新的列的名称。不需要包含表名，例如 `UPDATE t1 SET t1.col = 1` 是不合法的。

`expression`

给列赋值的表达式。

`where_condition`

只有满足 WHERE 条件的行才会被更新。该参数为必选，防止误更新整张表。如需更新整张表，请使用 `WHERE true`。

## 示例

创建表 `Employees` 来记录雇员信息，向表中插入五行数据。

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

如果需要对所有员工加薪 10%，则可以执行如下语句：

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- 将薪水增加10%
WHERE true;
```

如果需要对薪水低于平均薪水的员工加薪 10%，则可以执行如下语句，

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- 将薪水增加10%
WHERE Salary < (SELECT AVG(Salary) FROM Employees);
```

## 使用限制

2.3 ～ 2.5 版本，UPDATE 语法仅支持单表 UPDATE 且不支持公用表表达式（CTE）。
