# SUM

## Description

Returns the sum of non-null values for `expr`. You can use the DISTINCT keyword to compute the sum of distinct non-null values.

## Syntax

```Haskell
SUM([DISTINCT] expr)
```

## Parameters

`expr`: the expression that evaluates to a numeric value. Supported data types are TINYINT, SMALLINT, INT, FLOAT, DOUBLE, or DECIMAL.

## Return value

Data type mapping between input value and return value:

- TINYINT -> BIGINT
- SMALLINT -> BIGINT
- INT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- DECIMAL -> DECIMAL

## Usage notes

- This function ignores nulls.
- An error is returned if `expr` does not exist.
- If a VARCHAR expression is passed, this function implicitly casts the input into DOUBLE values. If the cast fails, an error is returned.

## Examples

1. Create a table named `employees`.

    ```SQL
    CREATE TABLE IF NOT EXISTS employees (
        region_num    TINYINT        COMMENT "range [-128, 127]",
        id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
        hobby         STRING         NOT NULL COMMENT "upper limit value 65533 bytes",
        income        DOUBLE         COMMENT "8 bytes",
        sales       DECIMAL(12,4)  COMMENT ""
        )
        DISTRIBUTED BY HASH(region_num) BUCKETS 8;
    ```

2. Insert data into `employees`.

    ```SQL
    INSERT INTO employees VALUES
    (3,432175,'3',25600,1250.23),
    (4,567832,'3',37932,2564.33),
    (3,777326,'2',null,1932.99),
    (5,342611,'6',43727,45235.1),
    (2,403882,'4',36789,52872.4);
    ```

3. Query data from `employees`.

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

4. Use this function to compute sum.

    Example 1: Calculate the total sales of each region.

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

    Example 2: Calculate the total employee income of each region. This function ignores nulls and the income of employee id `777326` is not counted.

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

    Example 3: Calculate the total number of hobbies. The `hobby` column is of the STRING type and will be implicitly converted to DOUBLE during computation.

    ```Plain Text
    MySQL > select sum(DISTINCT hobby) from employees;

    +---------------------+
    | sum(DISTINCT hobby) |
    +---------------------+
    |                  15 |
    +---------------------+
    1 row in set (0.01 sec)
    ```

    Example 4: Use `sum` with the WHERE clause to calculate the total income of employees whose monthly income is higher than 30000.

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
