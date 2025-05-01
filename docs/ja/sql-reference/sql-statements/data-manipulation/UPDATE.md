---
displayed_sidebar: docs
---

# UPDATE

主キーテーブルの行を更新します。

StarRocks は v2.3 から UPDATE ステートメントをサポートしており、単一テーブルの UPDATE のみをサポートし、共通テーブル式 (CTE) はサポートしていません。

## Syntax

```SQL
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    WHERE <where_condition>
```

## Parameters

`table_name`

更新するテーブルの名前。

`column_name`

更新する列の名前。テーブル名を含めることはできません。例えば、'UPDATE t1 SET col = 1' は無効です。

`expression`

列に新しい値を割り当てる式。

`where_condition`

行を更新する条件。この条件を満たす行のみが更新されます。このパラメータは必須です。テーブル全体を誤って更新しないようにするためです。テーブル全体を更新したい場合は、'WHERE true' を使用できます。

## Examples

従業員情報を記録するために `Employees` テーブルを作成し、5 行のデータを挿入します。

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

全従業員に 10% の昇給を行う必要がある場合、次のステートメントを実行できます。

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- 給与を 10% 増加させます。
WHERE true;
```

平均給与より低い給与の従業員に 10% の昇給を行う必要がある場合、次のステートメントを実行できます。

```SQL
UPDATE Employees
SET Salary = Salary * 1.1   -- 給与を 10% 増加させます。
WHERE Salary < (SELECT AVG(Salary) FROM Employees);
```

## Limitations

v2.3 から v2.5 まで、UPDATE ステートメントは単一テーブルの UPDATE のみをサポートし、共通テーブル式 (CTE) はサポートしていません。