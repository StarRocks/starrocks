---
displayed_sidebar: docs
---

# min_n

式から `n` 個の最小値を昇順でソートされた配列として返します。

この関数は v3.4 からサポートされています。

## Syntax

```Haskell
MIN_N(<expr>, <n>)
```

## Parameters

- `expr`: 任意のソート可能なタイプ (STRING, BOOLEAN, DATE, DATETIME, または数値タイプ) の式。
- `n`: 0 より大きい INTEGER リテラル。最大値は `10000` です。

## Return value

式から `n` 個の最小値を含む ARRAY を昇順でソートして返します。配列の要素は入力式と同じタイプです。

- 異なる値の数が `n` より少ない場合、すべての値を昇順でソートして返します。
- NULL 値は無視されます。
- すべての値が NULL の場合、空の配列を返します。

## Examples

1. テーブル `scores` を作成します。

    ```SQL
    CREATE TABLE scores (
        student_id INT,
        subject STRING,
        score INT
    ) DISTRIBUTED BY HASH(`student_id`);
    ```

2. このテーブルに値を挿入します。

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

3. 3 つの最小スコアを取得します。

    ```SQL
    SELECT MIN_N(score, 3) AS min_3_scores FROM scores;
    +----------------+
    | min_3_scores   |
    +----------------+
    | [78,82,85]     |
    +----------------+
    ```

4. 科目ごとの最小スコアを取得します。

    ```SQL
    SELECT subject, MIN_N(score, 2) AS min_2_scores 
    FROM scores 
    GROUP BY subject;
    +---------+---------------+
    | subject | min_2_scores  |
    +---------+---------------+
    | math    | [78,82]       |
    +---------+---------------+
    ```

5. 文字列値でテストします。

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

    SELECT MIN_N(name, 3) AS min_3_names FROM products;
    +------------------+
    | min_3_names      |
    +------------------+
    | [apple,banana,cherry] |
    +------------------+
    ```

6. n が値の数より大きい場合のエッジケースをテストします。

    ```SQL
    SELECT MIN_N(score, 15) AS all_scores FROM scores;
    +----------------------------------+
    | all_scores                       |
    +----------------------------------+
    | [78,82,85,87,88,89,90,92,93,95] |
    +----------------------------------+
    ```

7. 日付値でテストします。

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

    SELECT MIN_N(event_date, 3) AS earliest_3_dates FROM events;
    +----------------------------------+
    | earliest_3_dates                 |
    +----------------------------------+
    | [2024-01-20,2024-02-28,2024-03-15] |
    +----------------------------------+
    ```

## keyword

MIN_N