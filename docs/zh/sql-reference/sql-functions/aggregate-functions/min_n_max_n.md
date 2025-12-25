---
displayed_sidebar: docs
---

# MIN_N, MAX_N

返回表达式中最小或最大的 `n` 个值，以数组形式返回。

这些函数自 v4.0 起支持。

## 语法

```Haskell
MIN_N(<expr>, <n>)
MAX_N(<expr>, <n>)
```

## 参数说明

- `expr`: 任何可排序类型的表达式（STRING、BOOLEAN、DATE、DATETIME 或数值类型）。
- `n`: 大于 0 的 INTEGER 字面量。最大值为 `10000`。

## 返回值说明

- `MIN_N`: 返回一个数组，包含表达式中最小的 `n` 个值，按升序排列。
- `MAX_N`: 返回一个数组，包含表达式中最大的 `n` 个值，按升序排列。

数组元素的类型与输入表达式相同。

- 如果不同值的数量小于 `n`，返回所有值。
- NULL 值会被忽略。
- 如果所有值都是 NULL，返回空数组。

## 示例

### MIN_N 和 MAX_N 基本用法

1. 创建表 `scores`。

    ```SQL
    CREATE TABLE scores (
        student_id INT,
        subject STRING,
        score INT
    ) DISTRIBUTED BY HASH(`student_id`);
    ```

2. 向表中插入数据。

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

3. 获取最小和最大的 3 个分数。

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

4. 按科目获取最小和最大分数。

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

### 使用字符串值

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

### 边界情况：n 大于值的数量

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

### 使用日期值

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

## 关键字

MIN_N, MAX_N

