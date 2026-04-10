---
displayed_sidebar: docs
---

# MIN_N, MAX_N

式から `n` 個の最小値または最大値を配列として返します。

これらの関数は v4.0 からサポートされています。

## 構文

```Haskell
MIN_N(<expr>, <n>)
MAX_N(<expr>, <n>)
```

## パラメータ

- `expr`: 任意のソート可能なタイプ (STRING, BOOLEAN, DATE, DATETIME, または数値タイプ) の式。
- `n`: 0 より大きい INTEGER リテラル。最大値は `10000` です。

## 戻り値

- `MIN_N`: 式から `n` 個の最小値を含む配列を返し、昇順にソートします。
- `MAX_N`: 式から `n` 個の最大値を含む配列を返し、昇順にソートします。

配列の要素は入力式と同じタイプです。

- 異なる値の数が `n` より少ない場合、すべての値を返します。
- NULL 値は無視されます。
- すべての値が NULL の場合、空の配列を返します。

## 例

### MIN_N と MAX_N の基本的な使い方

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

3. 最小と最大の 3 つのスコアを取得します。

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

4. 科目ごとの最小と最大のスコアを取得します。

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

### 文字列値での使用

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

### エッジケース：n が値の数より大きい場合

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

### 日付値での使用

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

