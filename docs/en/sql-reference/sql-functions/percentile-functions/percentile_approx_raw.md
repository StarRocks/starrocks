---
displayed_sidebar: "English"
---

# percentile_approx_raw

## Description

Returns the value that corresponds to a specified percentile from `x`.

If `x` is a column, this function first sorts values in `x` in ascending order and returns the value that corresponds to percentile `y`.

## Syntax

```Haskell
PERCENTILE_APPROX_RAW(x, y);
```

## Parameters

- `x`: It can be a column or a set of values. It must evaluate to PERCENTILE.

- `y`: the percentile. The supported data type is DOUBLE. Value range: [0.0,1.0].

## Return value

Returns a PERCENTILE value.

## Examples

 Create table `aggregate_tbl`, where the `percent` column is the input of percentile_approx_raw().

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
  DISTRIBUTED BY HASH(`site_id`) BUCKETS 8
  PROPERTIES ("replication_num" = "1");
  ```

Insert data into the table.

  ```sql
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(1));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(2));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(3));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(4));
  ```

Calculate the value corresponding to percentile 0.5.

  ```Plain Text
  mysql> select percentile_approx_raw(percent, 0.5) from aggregate_tbl;
  +-------------------------------------+
  | percentile_approx_raw(percent, 0.5) |
  +-------------------------------------+
  |                                 2.5 |
  +-------------------------------------+
  1 row in set (0.03 sec)
  ```
