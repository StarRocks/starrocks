---
displayed_sidebar: docs
---

# sum

`expr` の非 NULL 値の合計を返します。DISTINCT キーワードを使用して、異なる非 NULL 値の合計を計算することができます。

## Syntax

```Haskell
SUM([DISTINCT] expr)
```

## Parameters

`expr`: 数値に評価される式。サポートされているデータ型は TINYINT、SMALLINT、INT、FLOAT、DOUBLE、または DECIMAL です。

## Return value

入力値と戻り値のデータ型のマッピング:

- TINYINT -> BIGINT
- SMALLINT -> BIGINT
- INT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- DECIMAL -> DECIMAL

## Usage notes

- この関数は NULL を無視します。
- `expr` が存在しない場合、エラーが返されます。
- VARCHAR 式が渡された場合、この関数は入力を DOUBLE 値に暗黙的にキャストします。キャストが失敗すると、エラーが返されます。

## Examples

1. `employees` という名前のテーブルを作成します。

    ```SQL
    CREATE TABLE IF NOT EXISTS employees (
        region_num    TINYINT        COMMENT "range [-128, 127]",
        id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
        hobby         STRING         NOT NULL COMMENT "upper limit value 65533 bytes",
        income        DOUBLE         COMMENT "8 bytes",
        sales       DECIMAL(12,4)  COMMENT ""
        )
        DISTRIBUTED BY HASH(region_num);
    ```

2. `employees` にデータを挿入します。

    ```SQL
    INSERT INTO employees VALUES
    (3,432175,'3',25600,1250.23),
    (4,567832,'3',37932,2564.33),
    (3,777326,'2',null,1932.99),
    (5,342611,'6',43727,45235.1),
    (2,403882,'4',36789,52872.4);
    ```

3. `employees` からデータをクエリします。

    ```Plain Text
    MySQL > select * from employees;
    +------------+--------+-------+--------+------------+
    | region_num | id     | hobby | income | sales      |
    +------------+--------+-------+--------+------------+
    |          5 | 342611 | 6     |  43727 | 45235.1000 |
    |          2 | 403882 | 4     |  36789 | 52872.4000 |
    |          4 | 567832 | 3     |  37932 |  2564.3300 |
    |          3 | 432175 | 3     |  25600 |  1250.2300 |
    |          3 | 777326 | 2     |   NULL |  1932.9900 |
    +------------+--------+-------+--------+------------+
    5 rows in set (0.01 sec)
    ```

4. この関数を使用して合計を計算します。

    例 1: 各地域の総売上を計算します。

    ```Plain Text
    MySQL > SELECT region_num, sum(sales) from employees
    group by region_num;

    +------------+------------+
    | region_num | sum(sales) |
    +------------+------------+
    |          2 | 52872.4000 |
    |          5 | 45235.1000 |
    |          4 |  2564.3300 |
    |          3 |  3183.2200 |
    +------------+------------+
    4 rows in set (0.01 sec)
    ```

    例 2: 各地域の従業員の総収入を計算します。この関数は NULL を無視し、従業員 ID `777326` の収入はカウントされません。

    ```Plain Text
    MySQL > select region_num, sum(income) from employees
    group by region_num;

    +------------+-------------+
    | region_num | sum(income) |
    +------------+-------------+
    |          2 |       36789 |
    |          5 |       43727 |
    |          4 |       37932 |
    |          3 |       25600 |
    +------------+-------------+
    4 rows in set (0.01 sec)
    ```

    例 3: 趣味の総数を計算します。`hobby` 列は STRING 型で、計算中に DOUBLE に暗黙的に変換されます。

    ```Plain Text
    MySQL > select sum(DISTINCT hobby) from employees;

    +---------------------+
    | sum(DISTINCT hobby) |
    +---------------------+
    |                  15 |
    +---------------------+
    1 row in set (0.01 sec)
    ```

    例 4: WHERE 句を使用して、月収が 30000 を超える従業員の総収入を計算します。

    ```Plain Text
    MySQL > select sum(income) from employees
    WHERE income > 30000;

    +-------------+
    | sum(income) |
    +-------------+
    |      118448 |
    +-------------+
    1 row in set (0.00 sec)
    ```

## keyword

SUM, sum