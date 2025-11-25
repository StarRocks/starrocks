---
displayed_sidebar: docs
---

# bool_or

Returns true if at least one row for `expr` is true. Otherwise, returns false. `boolor_agg` is an alias for this function.

## Syntax

```Haskell
bool_or(expr)
```

## Parameters

`expr`: The expression must evaluate to a boolean value, that is, true or false.

## Return value

Returns a boolean value. An error is returned if `expr` does not exist.

## Usage notes

This function ignores NULL values.

## Examples

1. Create a table named `employees`.

    ```SQL
    CREATE TABLE IF NOT EXISTS employees (
        region_num    TINYINT,
        id            BIGINT,
        is_manager    BOOLEAN
        )
        DISTRIBUTED BY HASH(region_num);
    ```

2. Insert data into `employees`.

    ```SQL
    INSERT INTO employees VALUES
    (3,432175, TRUE),
    (4,567832, FALSE),
    (3,777326, FALSE),
    (5,342611, TRUE),
    (2,403882, FALSE);
    ```

3. Query data from `employees`.

    ```Plain Text
    MySQL > select * from employees;
    +------------+--------+------------+
    | region_num | id     | is_manager |
    +------------+--------+------------+
    |          3 | 432175 |          1 |
    |          4 | 567832 |          0 |
    |          3 | 777326 |          0 |
    |          5 | 342611 |          1 |
    |          2 | 403882 |          0 |
    +------------+--------+------------+
    5 rows in set (0.01 sec)
    ```

4. Use this function to check for managers in each region.

    Example 1: Check if there is at least one manager in each region.

    ```Plain Text
    MySQL > SELECT region_num, bool_or(is_manager) from employees
    group by region_num;

    +------------+---------------------+
    | region_num | bool_or(is_manager) |
    +------------+---------------------+
    |          3 |                   1 |
    |          4 |                   0 |
    |          5 |                   1 |
    |          2 |                   0 |
    +------------+---------------------+
    4 rows in set (0.01 sec)
    ```

## keyword

BOOL_OR, bool_or, boolor_agg
