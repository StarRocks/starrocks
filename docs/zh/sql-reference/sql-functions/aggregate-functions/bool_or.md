---
displayed_sidebar: docs
---

# bool_or

如果 `expr` 的某一行计算结果为 true，则返回 true。否则，返回 false。`boolor_agg` 是此函数的别名。

## 语法

```Haskell
bool_or(expr)
```

## 参数

`expr`: 该表达式的计算结果必须为布尔值，即 true 或 false。

## 返回值

返回一个布尔值。如果 `expr` 不存在，则返回错误。

## 使用说明

此函数忽略 NULL 值。

## 示例

1. 创建一个名为 `employees` 的表。

    ```SQL
    CREATE TABLE IF NOT EXISTS employees (
        region_num    TINYINT,
        id            BIGINT,
        is_manager    BOOLEAN
        )
        DISTRIBUTED BY HASH(region_num);
    ```

2. 向 `employees` 表中插入数据。

    ```SQL
    INSERT INTO employees VALUES
    (3,432175, TRUE),
    (4,567832, FALSE),
    (3,777326, FALSE),
    (5,342611, TRUE),
    (2,403882, FALSE);
    ```

3. 从 `employees` 表中查询数据。

    ```Plain Text
    MySQL > select * from employees;
    +------------+--------+------------+
    | region_num | id     | is_manager |
    +------------+--------+------------+
    |          3 | 432175 |          1 |
    |          4 | 567832 |          0 |
    |          3 | 777326 |          0 |
    |          5 | 342611 |          1 |
    |          2 | 403882 |          0 |
    +------------+--------+------------+
    5 rows in set (0.01 sec)
    ```

4. 使用此函数检查每个区域是否有经理。

    示例 1: 检查每个区域是否至少有一名经理。

    ```Plain Text
    MySQL > SELECT region_num, bool_or(is_manager) from employees
    group by region_num;

    +------------+---------------------+
    | region_num | bool_or(is_manager) |
    +------------+---------------------+
    |          3 |                   1 |
    |          4 |                   0 |
    |          5 |                   1 |
    |          2 |                   0 |
    +------------+---------------------+
    4 rows in set (0.01 sec)
    ```

## 关键词

BOOL_OR, bool_or, boolor_agg
