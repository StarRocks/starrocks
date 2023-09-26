# l2_distance

## Description

Measures the distance between two vectors by calculating the Euclidean distance between them. The Euclidean distance, or L2 distance, is the sum of the squared differences between corresponding elements of the two vectors.

The distance is always non-negative and is zero if and only if the two vectors are identical.

l2_distance is commonly used for assessing similarity in various fields, including image processing, machine learning, and data analysis.

This function does not require the input vectors to be normalized.

The l2_distance used here refers to the distance without taking the square root, which is consistent with the behavior in faiss.

## Syntax

```Haskell
l2_distance(a, b)
```

## Parameters

`a` and `b` are the vectors to compare. They must have the same dimension. The supported data type is `Array<float>`. The two arrays must have the same number of elements. Otherwise, an error is returned.

## Return value

Returns a FLOAT value that is always non-negative. If any input parameter is null or invalid, an error is reported.

## Examples

1. Create a table to store vectors and insert data into this table.

    ```SQL
    CREATE TABLE test 
    (id int, data array<float>)
    DISTRIBUTED BY HASH(id);

    INSERT INTO test VALUES
    (1, array<float>[0.1, 0.2, 0.3]), 
    (2, array<float>[0.2, 0.1, 0.3]), 
    (3, array<float>[0.3, 0.2, 0.1]);
    ```

2. Calculate the similarity of each row in the `data` column compared to the array `[0.1, 0.2, 0.3]` and list the result in descending order.

    ```Plain
    SELECT id, data, l2_distance([0.1, 0.2, 0.3], data) as dist
    FROM test 
    ORDER BY dist DESC;

    +------+---------------+-------------+
    | id   | data          | dist        |
    +------+---------------+-------------+
    |    3 | [0.1,0.2,0.3] | 0.08000001  |
    |    2 | [0.2,0.1,0.3] | 0.020000001 |
    |    1 | [0.3,0.2,0.1] | 0           |
    +------+---------------+-------------+
    ```
