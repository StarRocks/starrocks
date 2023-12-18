---
displayed_sidebar: "Chinese"
---

# max_by

## 功能

返回与 `y` 的最大值相关联的 `x` 值。比如 `SELECT max_by(subject, exam_result) FROM exam;` 表示返回 `exam` 表中考试得分最高的科目。该函数从 2.5 版本开始支持。

## 语法

```Haskell
max_by(x,y)
```

## 参数说明

- `x`: 一个任意类型的表达式。

- `y`: 可以排序的某个类型的表达式。

## 返回值说明

返回值的类型与 `x` 相同。

## 使用说明

- `y` 必须是可排序的数据类型。如果 `y` 不可排序，比如是 BITMAP 或者 HLL 类型，返回报错。

- `y` 值包含 NULL 时，NULL 对应的行不参与计算。

- 如果存在多行都有最大值，则返回最先出现的那个 `x` 值。

## 示例

1. 创建表 `exam`。

    ```SQL
    CREATE TABLE exam (
        subject_id INT,
        subject STRING,
        exam_result INT
    ) DISTRIBUTED BY HASH(`subject_id`);
    ```

2. 向表插入数据并查询表中数据。

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

3. 返回得分最高的 1 个科目。

   可以看到有 2 个科目 `physics` 和 `music` 都是最高分 95。返回第一个出现的科目 `physics`。

    ```Plain
    SELECT max_by(subject, exam_result) FROM exam;
    +------------------------------+
    | max_by(subject, exam_result) |
    +------------------------------+
    | physics                      |
    +------------------------------+
    1 row in set (0.02 sec)
    ```
