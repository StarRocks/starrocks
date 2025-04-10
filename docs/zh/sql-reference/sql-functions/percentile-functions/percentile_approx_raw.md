---
displayed_sidebar: docs
---

# percentile_approx_raw

## 功能

计算给定参数 `x` 的百分位数。`x` 可以是列或者数值。如果 `x` 是列，则先对该列进行升序排序，然后取精确的第 `y` 位百分数。

## 语法

```Haskell
PERCENTILE_APPROX_RAW(x, y);
```

## 参数说明

`x`: 可以为列或数值。支持的数据类型为 PERCENTILE。

`y`: 指定的百分位，支持的数据类型为 DOUBLE，取值为 [0.0,1.0]。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

创建一张表 `aggregate_tbl`。其中 `percent` 列为 percentile_approx_raw() 函数的输入。

  ```sql
  CREATE TABLE `aggregate_tbl` (
    `site_id` largeint(40) NOT NULL COMMENT "id of site",
    `date` date NOT NULL COMMENT "time of event",
    `city_code` varchar(20) NULL COMMENT "city_code of user",
    `pv` bigint(20) SUM NULL DEFAULT "0" COMMENT "total page views",
    `percent` PERCENTILE PERCENTILE_UNION COMMENT "others"
  ) ENGINE=OLAP
  AGGREGATE KEY(`site_id`, `date`, `city_code`)
  COMMENT "OLAP"
  DISTRIBUTED BY HASH(`site_id`)
  PROPERTIES ("replication_num" = "3");
  ```

向表中插入数据。

  ```sql
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(1));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(2));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(3));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(4));
  ```

计算第 0.5 百分位对应的值。

  ```Plain Text
  mysql> select percentile_approx_raw(percent, 0.5) from aggregate_tbl;
  +-------------------------------------+
  | percentile_approx_raw(percent, 0.5) |
  +-------------------------------------+
  |                                 2.5 |
  +-------------------------------------+
  1 row in set (0.03 sec)
  ```
