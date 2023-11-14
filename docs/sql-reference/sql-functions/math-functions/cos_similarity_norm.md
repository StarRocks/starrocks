# cos_similarity_norm

## Description

Measures the similarity of two normalized vectors by calculating the cosine of the angle between them. The angle is formed by the direction of the vectors while the difference in their magnitude is ignored. This function assumes that the input vectors have been normalized. If you need to normalize vectors before calculating the cosine similarity, use [cosine_similarity](./cos_similarity.md).

The similarity is between -1 and 1. Smaller angles between vectors indicate greater cosine similarity.

- If two vectors have the same direction, they have a 0-degree angle and the cosine similarity is 1.
- Perpendicular vectors have a 90-degree angle and a cosine similarity of 0.
- Opposite vectors have a 180-degree angle and a cosine similarity of -1.

## Syntax

```Haskell
cosine_similarity_norm(a, b)
```

## Parameters

`a` and `b` are the vectors to compare. They must have the same dimension. The supported data type is `Array<float>`. The two arrays must have the same number of elements. Otherwise, an error is returned.

## Return value

Returns a FLOAT value within the range of [-1, 1]. If any input parameter is null or invalid, an error is reported.

## Examples

1. Create a table to store vectors and insert data into this table.

    ```SQL
    CREATE TABLE t1_similarity 
    (id int, data array<float>)
    DISTRIBUTED BY HASH(id);

    INSERT INTO t1_similarity VALUES
    (1, array<float>[0.1, 0.2, 0.3]), 
    (2, array<float>[0.2, 0.1, 0.3]), 
    (3, array<float>[0.3, 0.2, 0.1]);
    ```

2. Calculate the similarity of each row in the `data` column compared to the array `[0.1, 0.2, 0.3]` and list the result in descending order.

    ```Plain
    SELECT id, data, cosine_similarity_norm([0.1, 0.2, 0.3], data) as dist
    FROM t1_similarity 
    ORDER BY dist DESC;

    +------+---------------+------------+
    | id   | data          | dist       |
    +------+---------------+------------+
    |    1 | [0.1,0.2,0.3] | 0.14000002 |
    |    2 | [0.2,0.1,0.3] | 0.13000001 |
    |    3 | [0.3,0.2,0.1] | 0.10000001 |
    +------+---------------+------------+
    ```
