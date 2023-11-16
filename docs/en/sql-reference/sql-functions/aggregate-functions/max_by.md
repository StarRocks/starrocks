---
displayed_sidebar: "English"
---

# max_by

## Description

Returns the value of `x` associated with the maximum value of `y`.

For example, `SELECT max_by(subject, exam_result) FROM exam;` is to return the subject that has the highest exam score.

This function is supported from v2.5.

## Syntax

```Haskell
max_by(x,y)
```

## Parameters

- `x`: an expression of any type.
- `y`: an expression of a type that can be ordered.

## Return value

Returns a value that has the same type as `x`.

## Usage notes

- `y` must be a sortable type. If you use an unsortable type of `y`, such as `bitmap` or `hll`, an error is returned.
- If `y` contains a null value, the row that corresponds to the null value is ignored.
- If more than one value of `x` has the same maximum value of `y`, this function returns the first value of `x` encountered.

## Examples

1. Create a table `exam`.

    ```SQL
    CREATE TABLE exam (
        subject_id INT,
        subject STRING,
        exam_result INT
    ) DISTRIBUTED BY HASH(`subject_id`) BUCKETS 1;
    ```

2. Insert values into this table and query data from this table.

    ```SQL
    insert into exam values
    (1,'math',90),
    (2,'english',70),
    (3,'physics',95),
    (4,'chemistry',85),
    (5,'music',95),
    (6,'biology',null);

    select * from exam order by subject_id;
    +------------+-----------+-------------+
    | subject_id | subject   | exam_result |
    +------------+-----------+-------------+
    |          1 | math      |          90 |
    |          2 | english   |          70 |
    |          3 | physics   |          95 |
    |          4 | chemistry |          85 |
    |          5 | music     |          95 |
    |          6 | biology   |        null |
    +------------+-----------+-------------+
    6 rows in set (0.03 sec)
    ```

3. Obtain the subject that has the highest score.
   Two subjects `physics` and `music` have the same highest score `95` and the first subject encountered (`physics`) is returned.

    ```Plain
    SELECT max_by(subject, exam_result) FROM exam;
    +------------------------------+
    | max_by(subject, exam_result) |
    +------------------------------+
    | physics                      |
    +------------------------------+
    1 row in set (0.01 sec)
    ```
