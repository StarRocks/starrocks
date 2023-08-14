# bitmap_agg

## Description

Aggregates values (excluding NULLs) in a column into a bitmap (multiple rows into one row).

## Syntax

```Haskell
BITMAP_AGG(col)
```

## Parameters

`col`: the column whose values you want to aggregate. It must evaluate to BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, and LARGEINT.

## Return value

Returns a value of the BITMAP type.

## Usage notes

If the value in a row is less than 0 or greater than 18446744073709551615, the value will be ignored and not added to the Bitmap (see Example 3).

## Examples

Take the following data table as an example:

```PlainText
mysql> CREATE TABLE t1_test (
    c1 int,
    c2 boolean,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "3");

INSERT INTO t1_test VALUES
    (1, true, 11, 111, 1111, 11111),
    (2, false, 22, 222, 2222, 22222),
    (3, true, 33, 333, 3333, 33333),
    (4, null, null, null, null, null),
    (5, -1, -11, -111, -1111, -11111),
    (6, null, null, null, null, "36893488147419103232");

select * from t1_test order by c1;
+------+------+------+------+-------+----------------------+
| c1   | c2   | c3   | c4   | c5    | c6                   |
+------+------+------+------+-------+----------------------+
|    1 |    1 |   11 |  111 |  1111 | 11111                |
|    2 |    0 |   22 |  222 |  2222 | 22222                |
|    3 |    1 |   33 |  333 |  3333 | 33333                |
|    4 | NULL | NULL | NULL |  NULL | NULL                 |
|    5 |    1 |  -11 | -111 | -1111 | -11111               |
|    6 | NULL | NULL | NULL |  NULL | 36893488147419103232 |
+------+------+------+------+-------+----------------------+
```

Example 1: Aggregate values in column `c1` into one bitmap.

```PlainText
mysql> select bitmap_to_string(bitmap_agg(c1)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c1)) |
+----------------------------------+
| 1,2,3,4,5,6                      |
+----------------------------------+
```

Example 2: Aggregate values in column `c2` into one bitmap (NULLs are ignored).

```PlainText
mysql> SELECT BITMAP_TO_STRING(BITMAP_AGG(c2)) FROM t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c2)) |
+----------------------------------+
| 0,1                              |
+----------------------------------+
```

Example 3: Aggregate values in column `c6` into one bitmap (the last two values that exceed the value range are ignored).

```PlainText
mysql> select bitmap_to_string(bitmap_agg(c6)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c6)) |
+----------------------------------+
| 11111,22222,33333                |
+----------------------------------+
```

## Keywords

BITMAP_AGG, BITMAP
