
# sum

## description

返回指定字段所有值的总和。该函数会忽略 NULL 值，可以与 DISTINCT 运算符搭配使用。

## 语法

```Haskell
SUM(expr)
```

## 参数说明

`expr`: 表达式，用于指定列。列值可以为 TINYINT，INT，FLOAT，DOUBLE，或DECIMAL 类型。

## 返回值说明

列值和返回值类型的映射关系如下：

- TINYINT -> BIGINT
- INT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- DECIMAL -> DECIMAL

如果没有匹配的列，则返回报错。
如果某行值为NULL，该行不参与计算。
如果列值为 STRING 类型的数值，会隐式转换为 DOUBLE 类型后再进行运算。

## example

创建表`employees`。

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

向表中插入数据。

```SQL
INSERT INTO employees VALUES
(3,432175,'3',25600,1250.23),
(4,567832,'3',37932,2564.33),
(3,777326,'2',null,1932.99),
(5,342611,'6',43727,45235.1),
(2,403882,'4',36789,52872.4);
```

查询表数据。

```SQL
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

示例1：计算员工本月的销售额总和，以 `region_num` 进行分组。

```SQL
MySQL > SELECT region_num, sum(sales) from employees group by region_num;
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

示例2：计算员工月收入`income`，该函数会忽略null值。

```SQL
MySQL > select region_num, sum(income) from employees group by region_num;
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

示例3：结合 `DISTINCT`，计算员工爱好数总和。

```SQL
MySQL > select sum(DISTINCT hobby) from employees;
+---------------------+
| sum(DISTINCT hobby) |
+---------------------+
|                  15 |
+---------------------+
1 row in set (0.01 sec)
```

可以看到，`hobby`列为STRING类型的数值，在计算时会进行隐式转换。同时因使用了DISTINCT，计算时会去除重复值。

示例4：计算月收入大于30000的员工收入总和。

```SQL
MySQL > select sum(income) from employees
    -> WHERE income > 30000;
+-------------+
| sum(income) |
+-------------+
|      118448 |
+-------------+
1 row in set (0.00 sec)
```

## keyword

SUM
