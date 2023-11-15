
# sum

## 功能

对指定列的值进行相加。该函数会忽略 NULL 值，可以与 DISTINCT 运算符搭配使用。

## 语法

```Haskell
SUM(expr)
```

## 参数说明

`expr`: 用于指定参与求和运算的列。列值可以为 TINYINT，SMALLINT，INT，FLOAT，DOUBLE，或DECIMAL 类型。

## 返回值说明

列值和返回值类型的映射关系如下：

- TINYINT -> BIGINT
- SMALLINT -> BIGINT
- INT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- DECIMAL -> DECIMAL

如果没有匹配的列，则返回报错。
如果某行值为NULL，该行不参与计算。
如果列值为 STRING 类型的数值，会隐式转换为 DOUBLE 类型后再进行运算。

## 示例

1. 创建表`employees`。

    ```SQL
    CREATE TABLE IF NOT EXISTS employees (
        region_num    TINYINT        COMMENT "range [-128, 127]",
        id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
        hobby         STRING         NOT NULL COMMENT "upper limit value 65533 bytes",
        income        DOUBLE         COMMENT "8 bytes",
        sales       DECIMAL(12,4)  COMMENT ""
        )
        DISTRIBUTED BY HASH(region_num) BUCKETS 8;
    ```

2. 向表中插入数据。

    ```SQL
    INSERT INTO employees VALUES
    (3,432175,'3',25600,1250.23),
    (4,567832,'3',37932,2564.33),
    (3,777326,'2',null,1932.99),
    (5,342611,'6',43727,45235.1),
    (2,403882,'4',36789,52872.4);
    ```

3. 查询表数据。

    ```Plain Text
    MySQL > select * from employees;
    +------------+--------+-------+--------+------------+
    | region_num | id     | hobby | income | sales      |
    +------------+--------+-------+--------+------------+
    |          5 | 342611 | 6     |  43727 | 45235.1000 |
    |          2 | 403882 | 4     |  36789 | 52872.4000 |
    |          4 | 567832 | 3     |  37932 |  2564.3300 |
    |          3 | 432175 | 3     |  25600 |  1250.2300 |
    |          3 | 777326 | 2     |   NULL |  1932.9900 |
    +------------+--------+-------+--------+------------+
    5 rows in set (0.01 sec)
    ```

4. 使用`sum`函数进行求和运算。

    示例1：计算各地区的销售额总和，即以`region_num`进行分组对`sales`求和。

    ```Plain Text
    MySQL > SELECT region_num, sum(sales) from employees
    group by region_num;

    +------------+------------+
    | region_num | sum(sales) |
    +------------+------------+
    |          2 | 52872.4000 |
    |          5 | 45235.1000 |
    |          4 |  2564.3300 |
    |          3 |  3183.2200 |
    +------------+------------+
    4 rows in set (0.01 sec)
    ```

    示例2：计算各地区员工的收入总合，即以`region_num`进行分组对`income`进行求和。<br />因为 `sum` 函数忽略 NULL 值，因此`id`为`777326`的员工收入没有参与计算。

    ```Plain Text
    MySQL > select region_num, sum(income) from employees
    group by region_num;

    +------------+-------------+
    | region_num | sum(income) |
    +------------+-------------+
    |          2 |       36789 |
    |          5 |       43727 |
    |          4 |       37932 |
    |          3 |       25600 |
    +------------+-------------+
    4 rows in set (0.01 sec)
    ```

    示例3：计算员工爱好数总和。其中`hobby`列为STRING类型的数字，在计算时会进行隐式转换为DOUBLE类型。

    ```Plain Text
    MySQL > select sum(DISTINCT hobby) from employees;

    +---------------------+
    | sum(DISTINCT hobby) |
    +---------------------+
    |                  15 |
    +---------------------+
    1 row in set (0.01 sec)
    ```

    示例4：结合 WHERE 条件子句，计算月收入大于30000的员工收入总和。

    ```Plain Text
    MySQL > select sum(income) from employees
    WHERE income > 30000;

    +-------------+
    | sum(income) |
    +-------------+
    |      118448 |
    +-------------+
    1 row in set (0.00 sec)
    ```
