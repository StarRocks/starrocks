---
displayed_sidebar: docs
---

# MIN_N, MAX_N

Returns the `n` smallest or largest values from an expression as an array.

These functions are supported from v4.0.

## Syntax

```Haskell
MIN_N(<expr>, <n>)
MAX_N(<expr>, <n>)
```

## Parameters

- `expr`: An expression of any sortable type (STRING, BOOLEAN, DATE, DATETIME, or numeric type).
- `n`: An INTEGER literal greater than 0. The maximum value is `10000`.

## Return value

- `MIN_N`: Returns an ARRAY containing the `n` smallest values from the expression, sorted in ascending order.
- `MAX_N`: Returns an ARRAY containing the `n` largest values from the expression, sorted in ascending order.

The array elements have the same type as the input expression.

- If the number of distinct values is less than `n`, returns all values.
- NULL values are ignored.
- If all values are NULL, returns an empty array.

## Examples

### Basic usage with MIN_N and MAX_N

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

3. Get the 3 smallest and 3 largest scores.

    ```SQL
    SELECT 
        MIN_N(score, 3) AS min_3_scores,
        MAX_N(score, 3) AS max_3_scores 
    FROM scores;
    +----------------+----------------+
    | min_3_scores   | max_3_scores   |
    +----------------+----------------+
    | [85,82,78]     | [90,92,95]     |
    +----------------+----------------+
    ```

4. Get the smallest and largest scores by subject.

    ```SQL
    SELECT 
        subject, 
        MIN_N(score, 2) AS min_2_scores,
        MAX_N(score, 2) AS max_2_scores 
    FROM scores 
    GROUP BY subject;
    +---------+---------------+---------------+
    | subject | min_2_scores  | max_2_scores  |
    +---------+---------------+---------------+
    | math    | [82,78]       | [92,95]       |
    +---------+---------------+---------------+
    ```

### Using with string values

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

SELECT 
    MIN_N(name, 3) AS min_3_names,
    MAX_N(name, 3) AS max_3_names 
FROM products;
+---------------------------+--------------------------+
| min_3_names               | max_3_names              |
+---------------------------+--------------------------+
| [cherry,banana,apple]     | [cherry,date,elderberry] |
+---------------------------+--------------------------+
```

### Edge case: n larger than number of values

```SQL
SELECT 
    MIN_N(score, 15) AS all_scores_min,
    MAX_N(score, 15) AS all_scores_max 
FROM scores;
+----------------------------------+----------------------------------+
| all_scores_min                   | all_scores_max                   |
+----------------------------------+----------------------------------+
| [95,93,92,90,89,88,87,85,82,78]  | [78,82,85,87,88,89,90,92,93,95]  |
+----------------------------------+----------------------------------+
```

### Using with date values

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

SELECT 
    MIN_N(event_date, 3) AS earliest_3_dates,
    MAX_N(event_date, 3) AS latest_3_dates 
FROM events;
+------------------------------------+------------------------------------+
| earliest_3_dates                   | latest_3_dates                     |
+------------------------------------+------------------------------------+
| [2024-03-15,2024-02-28,2024-01-20] | [2024-03-15,2024-04-05,2024-05-10] |
+------------------------------------+------------------------------------+
```

## keyword

MIN_N, MAX_N

