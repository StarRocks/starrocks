# UPDATE

该语句用于更新一张主键模型表中的数据行。

3.0 版本之前，UPDATE 语句仅支持单表 UPDATE 且不支持公用表表达式（CTE）。从 3.0 版本开始，StarRocks 丰富了 UPDATE 语法，支持使用多表关联和 CTE。如果需要将待更新的表与数据库中其他表关联，则可以在 FROM 子句或 CTE 中引用其他的表。从 3.1 版本开始，支持列模式的部分更新，适用于涉及少数列但是大量行的场景，加快更新速度。

## 使用说明

如果为多表 UPDATE，则您需要确保 UPDATE 语句中 FROM 子句的表表达式可以转换成等价的 JOIN 查询语句。因为 StarRocks 实际执行 UPDATE 语句时，内部会进行这样的转换。假设 UPDATE 语句为 `UPDATE t0 SET v1=t1.v1 FROM t1 WHERE t0.pk = t1.pk;`，该 FROM 子句的表表达式可以转换为 `t0 JOIN t1 ON t0.pk=t1.pk;`。并且 StarRocks 根据 JOIN 查询的结果集，匹配待更新表的数据行，更新其指定列的值。如果结果集中存在多行数据和待更新表的某一行数据相匹配，则待更新表中这行数据指定列更新后的值，是结果集多行数据中随机一行的指定列的值。

## 语法

**单表 UPDATE**

如果待更新表的数据行满足 WHERE 条件，则对该数据行的指定列赋予新值。

```SQL
[ WITH <with_query> [, ...] ]
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    WHERE <where_condition>
```

**多表 UPDATE**

基于多表关联查询的结果集与待更新的表进行匹配，如果待更新表的数据行匹配结果集并且满足 WHERE 条件，则对该数据行的指定列赋予新值。

```SQL
[ WITH <with_query> [, ...] ]
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    [ FROM <from_item> [, ...] ]
    WHERE <where_condition>
```

## 参数说明

`with_query`

一个或多个可以在 UPDATE 语句中通过名字引用的 CTE。CTE 是一个临时结果集，可以提高复杂语句的易读性。

`table_name`

待更新的表的名称。

`column_name`

待更新的列的名称。不需要包含表名，例如 `UPDATE t1 SET t1.col = 1` 是不合法的。

`expression`

给列赋值的表达式。

`from_item`

引用数据库中一个或者多个其他的表。该表与待更新的表进行连接，WHERE 子句指定连接条件，最终基于连接查询的结果集给待更新的表中匹配行的列赋值。 例如 FROM 子句为 `FROM t1 WHERE t0.pk = t1.pk;`，StarRocks 实际执行 UPDATE 语句时会将该 FROM 子句的表表达式会转换为 `t0 JOIN t1 ON t0.pk=t1.pk;`。

`where_condition`

只有满足 WHERE 条件的行才会被更新。该参数为必选，防止误更新整张表。如需更新整张表，请使用 `WHERE true`。如果使用[列模式的部分更新](#列模式的部分更新自-31)，则该参数不是必选。

## 列模式的部分更新（自 3.1）

列模式的部分更新适用于涉及少数列并且大量行的场景。在该场景，列模式相较于普通模式，更新速度更快。例如，在一个包含 100 列的表中，每次更新 10 列（占比 10%）并更新所有行，则相比普通模式，列模式的更新性能将提高10倍。

系统变量 `partial_update_mode` 用于控制部分更新的模式，支持取值为：

* `auto`（默认值），表示由系统通过分析更新语句以及其涉及的列，自动判断执行部分更新时使用的模式。如果满足如下标准，则系统自动使用列模式：
  * 更新的列数占所有列数的百分比小于 30%，并且更新的列数少于 4 个。
  * 更新语句中没有使用 WHERE 条件。
  反之，则系统自动使用普通模式。
* `column`，指定使用列模式执行部分更新，比较适用于涉及少数列并且大量行的部分列更新场景。

您可以使用 `EXPLAIN UPDATE xxx` 查看执行部分列更新的模式。

## 示例

### 单表 UPDATE

创建表 `Employees` 来记录雇员信息，向表中插入五行数据。

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

您也可以使用 CTE 改写上述语句，增加易读性。

```SQL
WITH AvgSalary AS (
    SELECT AVG(Salary) AS AverageSalary
    FROM Employees
)
UPDATE Employees
SET Salary = Salary * 1.1  -- 将薪水增加10%
FROM AvgSalary
WHERE Employees.Salary < AvgSalary.AverageSalary;
```

### 多表 UPDATE

创建表 `Accounts` 来记录账户信息，向表中插入三行数据。

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

如果需要给表 `Employees` 中 Acme Corporation 公司管理帐户的员工加薪 10%，则可以执行如下语句：

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- 将薪水增加10%
FROM Accounts
WHERE Accounts.name = 'Acme Corporation'
    AND Employees.Name = Accounts.Sales_person;
```

您也可以使用 CTE 改写上述语句，增加易读性。

```SQL
WITH Acme_Accounts as (
    SELECT * from Accounts
    WHERE Accounts.name = 'Acme Corporation'
)
UPDATE Employees SET Salary = Salary * 1.1 -- 将薪水增加10%
FROM Acme_Accounts
WHERE Employees.Name = Acme_Accounts.Sales_person;
```
