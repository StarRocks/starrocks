---
displayed_sidebar: "English"
---

# retention

## Description

Calculates the user retention rate within a specified period of time. This function accepts 1 to 31 conditions and evaluates whether each condition is true. If the condition evaluates to true, 1 is returned. Otherwise, 0 is returned. It eventually returns an array of 0 and 1. You can calculate the user retention rate based on this data.

## Syntax

```Haskell
ARRAY retention(ARRAY input)
```

## Parameters

`input`: an array of conditions. A maximum of 31 conditions can be passed in. Separate multiple conditions with commas.

## Return value

Returns an array of 0 and 1. The number of 0 and 1 is the same as the number of input conditions.

The evaluation starts from the first condition.

- If the condition evaluates to true, 1 is returned. Otherwise, 0 is returned.
- If the first condition is not true, the current position and its following positions are all set to 0.

## Examples

1. Create a table named `test` and insert data.

    ```SQL
    CREATE TABLE test(
        id TINYINT,
        action STRING,
        time DATETIME
    )
    ENGINE=olap
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id);

    INSERT INTO test VALUES 
    (1,'pv','2022-01-01 08:00:05'),
    (2,'pv','2022-01-01 10:20:08'),
    (1,'buy','2022-01-02 15:30:10'),
    (2,'pv','2022-01-02 17:30:05'),
    (3,'buy','2022-01-01 05:30:09'),
    (3,'buy','22022-01-02 08:10:15'),
    (4,'pv','2022-01-02 21:09:15'),
    (5,'pv','2022-01-01 22:10:53'),
    (5,'pv','2022-01-02 19:10:52'),
    (5,'buy','2022-01-02 20:00:50');
    ```

2. Query data from `test`.

    ```Plain Text
    MySQL > select * from test order by id;
    +------+--------+---------------------+
    | id   | action | time                |
    +------+--------+---------------------+
    |    1 | pv     | 2022-01-01 08:00:05 |
    |    1 | buy    | 2022-01-02 15:30:10 |
    |    2 | pv     | 2022-01-01 10:20:08 |
    |    2 | pv     | 2022-01-02 17:30:05 |
    |    3 | buy    | 2022-01-01 05:30:09 |
    |    3 | buy    | 2022-01-02 08:10:15 |
    |    4 | pv     | 2022-01-02 21:09:15 |
    |    5 | pv     | 2022-01-01 22:10:53 |
    |    5 | pv     | 2022-01-02 19:10:52 |
    |    5 | buy    | 2022-01-02 20:00:50 |
    +------+--------+---------------------+
    10 rows in set (0.01 sec)
    ```

3. Use `retention` to calculate user retention rate.

    Example 1: Evaluate user behavior against the following conditions: view commodity page on 2022-01-01 (action='pv') and place an order on 2022-01-02 (action='buy').

    ```Plain Text
    MySQL > select id, retention([action='pv' and to_date(time)='2022-01-01',
                                  action='buy' and to_date(time)='2022-01-02']) as retention 
    from test 
    group by id
    order by id;
    
    +------+-----------+
    | id   | retention |
    +------+-----------+
    |    1 | [1,1]     |
    |    2 | [1,0]     |
    |    3 | [0,0]     |
    |    4 | [0,0]     |
    |    5 | [1,1]     |
    +------+-----------+
    5 rows in set (0.01 sec)
    ```

    In the result:

    - Users 1 and 5 meets two conditions and [1,1] is returned.

    - User 2 does not meet the second condition and [1,0] is returned.

    - User 3 meets the second condition but does not meet the first condition. [0,0] is returned.

    - User 4 meets no condition and [0,0] is returned.

    Example 2: Calculate the percentage of users who have viewed commodity page on 2022-01-01 (action='pv') and placed an order on 2022-01-02 (action='buy').

    ```Plain Text
    MySQL > select sum(r[1]),sum(r[2])/sum(r[1])
    from (select id, retention([action='pv' and to_date(time)='2022-01-01',
                                action='buy' and to_date(time)='2022-01-02']) as r 
    from test 
    group by id 
    order by id) t;
    
    +-----------+---------------------------+
    | sum(r[1]) | (sum(r[2])) / (sum(r[1])) |
    +-----------+---------------------------+
    |         3 |        0.6666666666666666 |
    +-----------+---------------------------+
    1 row in set (0.02 sec)
    ```

    The return value is the user retention rate on 2022-01-02.

## keyword

retention, retention rate, RETENTION
