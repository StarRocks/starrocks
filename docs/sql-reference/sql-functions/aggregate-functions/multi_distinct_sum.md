# MAX

## Description

Return the sum value of the distinct expr, equivalent to sum(distinct expr)

## Syntax

```Haskell
multi_distinct_sum(expr)
```

## Parameters

- `expr`: Used to specify the columns involved in the operation. Column values can be of type TINYINT, SMALLINT, INT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

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

```plain text
create table with `k0` int field
MySQL > CREATE TABLE tabl
(k0 BIGINT NOT NULL) ENGINE=OLAP
DUPLICATE KEY(`k0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k0`) BUCKETS 1
PROPERTIES(
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
Query OK, 0 rows affected (0.01 sec)

insert values 0,1,1,1,2,2
MySQL > INSERT INTO tabl VALUES ('0'), ('1'), ('1'), ('1'), ('2'), ('2');
Query OK, 6 rows affected (0.15 sec)

The distinct values of k0 is 0,1,2 and wo can get 3 after adding them together
MySQL > select multi_distinct_sum(k0) from tabl;
+------------------------+
| multi_distinct_sum(k0) |
+------------------------+
|                      3 |
+------------------------+
1 row in set (0.03 sec)
```