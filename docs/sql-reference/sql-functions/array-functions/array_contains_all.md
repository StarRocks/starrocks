# array_contains_all

## Description

Checks whether `arr1` contains all the elements of `arr2`, that is, whether `arr2` is a subset of `arr1`. If yes, 1 is returned. If not, 0 is returned.

## Syntax

~~~Haskell
BOOLEAN array_contains_all(arr1, arr2)
~~~

## Parameters

`arr`: the two arrays to compare. This syntax checks whether `arr2` is a subset of `arr1`.

The data types of elements in the two arrays must be the same. For the data types of array elements supported by StarRocks, see [ARRAY](../../../sql-reference/sql-statements/data-types/Array.md).

## Return value

Returns a value of the BOOLEAN type.

1 is returned if `arr2` is a subset of `arr1`. Otherwise, 0 is returned.

If any of the two arrays is NULL, NULL is returned.

## Usage notes

- If an array contains `null` elements, `null` is processed as a value.

- An empty array is a subset of any array.

- Elements in the two arrays can have different order.

## Examples

1. Create a table named `t1` and insert data into this table.

    ~~~SQL
    CREATE TABLE t1 (
        c0 INT,
        c1 ARRAY<INT>,
        c2 ARRAY<INT>
    ) ENGINE=OLAP
    DUPLICATE KEY(c0)
    DISTRIBUTED BY HASH(c0) BUCKETS 8;

    INSERT INTO t1 VALUES
        (1,[1,2,3],[1,2]),
        (2,[1,2,3],[1,4]),
        (3,NULL,[1]),
        (4,[1,2,null],NULL),
        (5,[1,2,null],[null]),
        (6,[2,3],[]);
    ~~~

2. Query data from this table.

    ~~~Plain
    SELECT * FROM t1 ORDER BY c0;
    +------+------------+----------+
    | c0   | c1         | c2       |
    +------+------------+----------+
    |    1 | [1,2,3]    | [1,2]    |
    |    2 | [1,2,3]    | [1,4]    |
    |    3 | NULL       | [1]      |
    |    4 | [1,2,null] | NULL     |
    |    5 | [1,2,null] | [null]   |
    |    6 | [2,3]      | []       |
    +------+------------+----------+
    ~~~

3. Check whether each row of `c2` is a subset of the corresponding row of `c1`.

    ~~~Plaintext
    SELECT c0, c1, c2, array_contains_all(c1, c2) FROM t1 ORDER BY c0;
    +------+------------+----------+----------------------------+
    | c0   | c1         | c2       | array_contains_all(c1, c2) |
    +------+------------+----------+----------------------------+
    |    1 | [1,2,3]    | [1,2]    |                          1 |
    |    2 | [1,2,3]    | [1,4]    |                          0 |
    |    3 | NULL       | [1]      |                       NULL |
    |    4 | [1,2,null] | NULL     |                       NULL |
    |    5 | [1,2,null] | [null]   |                          1 |
    |    6 | [2,3]      | []       |                          1 |
    +------+------------+----------+----------------------------+
    ~~~

In the output:

For row 1, `c2` is a subset of `c1` and 1 is returned.

For row 2, `c2` is not a subset of `c1` and 0 is returned.

For row 3, `c1` is NULL and NULL is returned.

For row 4, `c2` is NULL and NULL is returned.

For row 5, the two arrays contain `null` and `null` is processed as a normal value, `1` is returned.

For row 6, `c2` is an empty array and considered a subset of `c1`. Therefore, `1` is returned.
