---
displayed_sidebar: docs
---

# max_n

返回表达式中最大的 `n` 个值，以数组形式返回，按升序排列。

该函数自 v3.4 起支持。

## 语法

```Haskell
MAX_N(<expr>, <n>)
```

## 参数说明

- `expr`: 任何可排序类型的表达式（STRING、BOOLEAN、DATE、DATETIME 或数值类型）。
- `n`: 大于 0 的 INTEGER 字面量。最大值为 `10000`。

## 返回值说明

返回一个数组，包含表达式中最大的 `n` 个值，按升序排列。数组元素的类型与输入表达式相同。

- 如果不同值的数量小于 `n`，返回所有值并按升序排列。
- NULL 值会被忽略。
- 如果所有值都是 NULL，返回空数组。

## 示例

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

3. 获取最大的 3 个分数。

    ```SQL
    SELECT MAX_N(score, 3) AS max_3_scores FROM scores;
    +----------------+
    | max_3_scores   |
    +----------------+
    | [90,92,95]     |
    +----------------+
    ```

4. 按科目获取最大分数。

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

5. 测试字符串值。

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

6. 测试边界情况，当 n 大于值的数量时。

    ```SQL
    SELECT MAX_N(score, 15) AS all_scores FROM scores;
    +----------------------------------+
    | all_scores                       |
    +----------------------------------+
    | [78,82,85,87,88,89,90,92,93,95] |
    +----------------------------------+
    ```

7. 测试日期值。

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

## 关键字

MAX_N
