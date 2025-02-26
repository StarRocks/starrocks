---
displayed_sidebar: docs
---

# UPDATE

主キーテーブルの行を更新します。

StarRocks は v2.3 から UPDATE 文をサポートしており、単一テーブルの UPDATE のみをサポートし、共通テーブル式 (CTE) はサポートしていません。バージョン 3.0 から、StarRocks は構文を拡張し、マルチテーブルのジョインと CTE をサポートします。データベース内の他のテーブルと更新対象のテーブルをジョインする必要がある場合、FROM 句または CTE でこれらの他のテーブルを参照できます。バージョン 3.1 以降、UPDATE 文は列モードでの部分更新をサポートしており、少数の列を含むが多数の行を含むシナリオに適しており、更新速度が速くなります。

このコマンドを実行するには、更新したいテーブルに対する UPDATE 権限が必要です。

## 使用上の注意

複数のテーブルを含む UPDATE 文を実行する際、StarRocks は UPDATE 文の FROM 句内のテーブル式を同等の JOIN クエリ文に変換します。したがって、UPDATE 文の FROM 句で指定するテーブル式がこの変換をサポートしていることを確認してください。例えば、UPDATE 文が 'UPDATE t0 SET v1=t1.v1 FROM t1 WHERE t0.pk = t1.pk;' の場合、FROM 句のテーブル式は 't0 JOIN t1 ON t0.pk=t1.pk;' に変換できます。StarRocks は、JOIN クエリの結果セットに基づいて更新するデータ行を一致させます。結果セットの複数の行が更新対象のテーブルの特定の行に一致する可能性があります。このシナリオでは、その行はこれらの複数の行の中のランダムな行の値に基づいて更新されます。

## 構文

### 単一テーブルの UPDATE

更新対象のテーブルのデータ行が WHERE 条件を満たす場合、これらのデータ行の指定された列に新しい値が割り当てられます。

```SQL
[ WITH <with_query> [, ...] ]
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    WHERE <where_condition>
```

### マルチテーブルの UPDATE

マルチテーブルのジョインからの結果セットが更新対象のテーブルと一致します。更新対象のテーブルのデータ行が結果セットと一致し、WHERE 条件を満たす場合、これらのデータ行の指定された列に新しい値が割り当てられます。

```SQL
[ WITH <with_query> [, ...] ]
UPDATE <table_name>
    SET <column_name> = <expression> [, ...]
    [ FROM <from_item> [, ...] ]
    WHERE <where_condition>
```

## パラメータ

`with_query`

UPDATE 文で名前で参照できる 1 つ以上の CTE。CTE は一時的な結果セットで、複雑な文の可読性を向上させることができます。

`table_name`

更新対象のテーブルの名前。

`column_name`

更新対象の列の名前。テーブル名を含めることはできません。例えば、'UPDATE t1 SET col = 1' は無効です。

`expression`

列に新しい値を割り当てる式。

`from_item`

データベース内の 1 つ以上の他のテーブル。これらのテーブルは、WHERE 句で指定された条件に基づいて更新対象のテーブルとジョインできます。結果セットの行の値は、更新対象のテーブルの一致する行の指定された列の値を更新するために使用されます。例えば、FROM 句が `FROM t1 WHERE t0.pk = t1.pk` の場合、StarRocks は UPDATE 文を実行する際に FROM 句のテーブル式を `t0 JOIN t1 ON t0.pk=t1.pk` に変換します。

`where_condition`

行を更新する基準となる条件。WHERE 条件を満たす行のみが更新されます。このパラメータは、テーブル全体を誤って更新するのを防ぐために必要です。テーブル全体を更新したい場合は、'WHERE true' を使用できます。ただし、このパラメータは [列モードでの部分更新](#partial-updates-in-column-mode-since-v31) には必要ありません。

## 列モードでの部分更新 (v3.1 以降)

列モードでの部分更新は、少数の列のみが更新されるが、多数の行が更新される必要があるシナリオに適しています。このようなシナリオでは、列モードを有効にすることで更新速度が速くなります。例えば、100 列のテーブルで、すべての行に対して 10 列 (全体の 10%) のみが更新される場合、列モードの更新速度は 10 倍速くなります。

システム変数 `partial_update_mode` は部分更新のモードを制御し、以下の値をサポートします。

- `auto` (デフォルト): システムは UPDATE 文と関与する列を分析して部分更新のモードを自動的に決定します。以下の条件が満たされる場合、システムは自動的に列モードを使用します。
  - 更新される列の割合が全体の 30% 未満で、更新される列の数が 4 未満である。
  - 更新文が WHERE 条件を使用していない。

  それ以外の場合、システムは列モードを使用しません。
- `column`: 部分更新に列モードを使用します。特に少数の列と多数の行を含む部分更新に適しています。

`EXPLAIN UPDATE xxx` を使用して部分更新のモードを確認できます。

## 例

### 単一テーブルの UPDATE

従業員情報を記録するためのテーブル `Employees` を作成し、5 行のデータをテーブルに挿入します。

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

すべての従業員に 10% の昇給を与える必要がある場合、次の文を実行できます。

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- 給与を 10% 増加させます。
WHERE true;
```

平均給与より低い給与の従業員に 10% の昇給を与える必要がある場合、次の文を実行できます。

```SQL
UPDATE Employees
SET Salary = Salary * 1.1   -- 給与を 10% 増加させます。
WHERE Salary < (SELECT AVG(Salary) FROM Employees);
```

また、CTE を使用して上記の文を再構成し、可読性を向上させることもできます。

```SQL
WITH AvgSalary AS (
    SELECT AVG(Salary) AS AverageSalary
    FROM Employees
)
UPDATE Employees
SET Salary = Salary * 1.1   -- 給与を 10% 増加させます。
FROM AvgSalary
WHERE Employees.Salary < AvgSalary.AverageSalary;
```

### マルチテーブルの UPDATE

アカウント情報を記録するためのテーブル `Accounts` を作成し、3 行のデータをテーブルに挿入します。

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

`Employees` テーブルの Acme Corporation のアカウントを管理する従業員に 10% の昇給を与える必要がある場合、次の文を実行できます。

```SQL
UPDATE Employees
SET Salary = Salary * 1.1  -- 給与を 10% 増加させます。
FROM Accounts
WHERE Accounts.name = 'Acme Corporation'
    AND Employees.Name = Accounts.Sales_person;
```

また、CTE を使用して上記の文を再構成し、可読性を向上させることもできます。

```SQL
WITH Acme_Accounts as (
    SELECT * from Accounts
    WHERE Accounts.name = 'Acme Corporation'
)
UPDATE Employees SET Salary = Salary * 1.1 -- 給与を 10% 増加させます。
FROM Acme_Accounts
WHERE Employees.Name = Acme_Accounts.Sales_person;
```