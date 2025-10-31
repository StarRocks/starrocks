---
displayed_sidebar: docs
---

# max_n

式から `n` 個の最大値を配列として返し、昇順にソートします。

この関数は v3.4 からサポートされています。

## 構文

```Haskell
MAX_N(<expr>, <n>)
```

## パラメータ

- `expr`: 任意のソート可能なタイプ (STRING, BOOLEAN, DATE, DATETIME, または数値タイプ) の式。
- `n`: 0 より大きい INTEGER リテラル。最大値は `10000` です。

## 戻り値

式から `n` 個の最大値を含む配列を返し、昇順にソートします。配列の要素は入力式と同じタイプです。

- 異なる値の数が `n` より少ない場合、すべての値を昇順にソートして返します。
- NULL 値は無視されます。
- すべての値が NULL の場合、空の配列を返します。

## 例

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

3. 3 つの最大スコアを取得します。

    ```SQL
    SELECT MAX_N(score, 3) AS max_3_scores FROM scores;
    +----------------+
    | max_3_scores   |
    +----------------+
    | [90,92,95]     |
    +----------------+
    ```

4. 科目ごとの最大スコアを取得します。

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

    SELECT MAX_N(name, 3) AS max_3_names FROM products;
    +--------------------------+
    | max_3_names              |
    +--------------------------+
    | [cherry,date,elderberry] |
    +--------------------------+
    ```

6. n が値の数より大きい場合のエッジケースをテストします。

    ```SQL
    SELECT MAX_N(score, 15) AS all_scores FROM scores;
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

    SELECT MAX_N(event_date, 3) AS latest_3_dates FROM events;
    +----------------------------------+
    | latest_3_dates                   |
    +----------------------------------+
    | [2024-03-15,2024-04-05,2024-05-10] |
    +----------------------------------+
    ```

## keyword

MAX_N