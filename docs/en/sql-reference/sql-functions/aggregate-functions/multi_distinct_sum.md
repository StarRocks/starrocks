---
displayed_sidebar: "English"
---

# multi_distinct_sum

## Description

Returns the sum of distinct values in `expr`, equivalent to sum(distinct expr).

## Syntax

```Haskell
multi_distinct_sum(expr)
```

## Parameters

- `expr`: the columns involved in the calculation. Column values can be of the following types: TINYINT, SMALLINT, INT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

## Return value

The mapping between column values and return value types is as follows:

- BOOLEAN -> BIGINT
- TINYINT -> BIGINT
- SMALLINT -> BIGINT
- INT -> BIGINT
- BIGINT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- LARGEINT -> LARGEINT
- DECIMALV2 -> DECIMALV2
- DECIMAL32 -> DECIMAL128
- DECIMAL64 -> DECIMAL128
- DECIMAL128 -> DECIMAL128

## Examples

1. Create a table with `k0` as the INT field.

    ```sql
    CREATE TABLE tabl
    (k0 BIGINT NOT NULL) ENGINE=OLAP
    DUPLICATE KEY(`k0`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k0`) BUCKETS 1
    PROPERTIES(
        "replication_num" = "3",
        "storage_format" = "DEFAULT"
    );
    ```

2. Insert values into the table.

    ```sql
    INSERT INTO tabl VALUES ('0'), ('1'), ('1'), ('1'), ('2'), ('2');
    ```

3. Use multi_distinct_sum() to calculate the sum of distinct values in the `k0` column.

    ```plain text
    MySQL > select multi_distinct_sum(k0) from tabl;
    +------------------------+
    | multi_distinct_sum(k0) |
    +------------------------+
    |                      3 |
    +------------------------+
    1 row in set (0.03 sec)
    ```

    The distinct values of `k0` is 0, 1, 2 and we can get 3 after adding them together.
