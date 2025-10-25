---
displayed_sidebar: docs
---

# max_n

Returns the `n` largest values from an expression as an array, sorted in ascending order.

This function is supported from v3.4.

## Syntax

```Haskell
MAX_N(<expr>, <n>)
```

## Parameters

- `expr`: An expression of any sortable type (STRING, BOOLEAN, DATE, DATETIME, or numeric type).
- `n`: An INTEGER literal greater than 0. The maximum value is `10000`.

## Return value

Returns an ARRAY containing the `n` largest values from the expression, sorted in ascending order. The array elements have the same type as the input expression.

- If the number of distinct values is less than `n`, returns all values sorted in ascending order.
- NULL values are ignored.
- If all values are NULL, returns an empty array.

## Examples

1. Create a table `scores`.

    ```SQL
    CREATE TABLE scores (
        student_id INT,
        subject STRING,
        score INT
    ) DISTRIBUTED BY HASH(`student_id`);
    ```

2. Insert values into this table.

    ```SQL
    INSERT INTO scores VALUES
    (1, 'math', 85),
    (2, 'math', 92),
    (3, 'math', 78),
    (4, 'math', 95),
    (5, 'math', 88),
    (6, 'math', 82),
    (7, 'math', 90),
    (8, 'math', 87),
    (9, 'math', 93),
    (10, 'math', 89);
    ```

3. Get the 3 largest scores.

    ```SQL
    SELECT MAX_N(score, 3) AS max_3_scores FROM scores;
    +----------------+
    | max_3_scores   |
    +----------------+
    | [90,92,95]     |
    +----------------+
    ```

4. Get the largest scores by subject.

    ```SQL
    SELECT subject, MAX_N(score, 2) AS max_2_scores 
    FROM scores 
    GROUP BY subject;
    +---------+---------------+
    | subject | max_2_scores  |
    +---------+---------------+
    | math    | [92,95]       |
    +---------+---------------+
    ```

5. Test with string values.

    ```SQL
    CREATE TABLE products (
        id INT,
        name STRING,
        price DECIMAL(10,2)
    ) DISTRIBUTED BY HASH(`id`);

    INSERT INTO products VALUES
    (1, 'apple', 2.50),
    (2, 'banana', 1.80),
    (3, 'cherry', 4.20),
    (4, 'date', 3.10),
    (5, 'elderberry', 5.50);

    SELECT MAX_N(name, 3) AS max_3_names FROM products;
    +--------------------------+
    | max_3_names              |
    +--------------------------+
    | [cherry,date,elderberry] |
    +--------------------------+
    ```

6. Test edge case where n is larger than the number of values.

    ```SQL
    SELECT MAX_N(score, 15) AS all_scores FROM scores;
    +----------------------------------+
    | all_scores                       |
    +----------------------------------+
    | [78,82,85,87,88,89,90,92,93,95] |
    +----------------------------------+
    ```

7. Test with date values.

    ```SQL
    CREATE TABLE events (
        id INT,
        event_name STRING,
        event_date DATE
    ) DISTRIBUTED BY HASH(`id`);

    INSERT INTO events VALUES
    (1, 'conference', '2024-03-15'),
    (2, 'workshop', '2024-01-20'),
    (3, 'seminar', '2024-05-10'),
    (4, 'meeting', '2024-02-28'),
    (5, 'training', '2024-04-05');

    SELECT MAX_N(event_date, 3) AS latest_3_dates FROM events;
    +----------------------------------+
    | latest_3_dates                   |
    +----------------------------------+
    | [2024-03-15,2024-04-05,2024-05-10] |
    +----------------------------------+
    ```

## keyword

MAX_N
