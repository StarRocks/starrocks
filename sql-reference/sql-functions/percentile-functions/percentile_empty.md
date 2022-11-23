# percentile_empty

## 功能

构造一个 `percentile` 类型的数值，主要用于 `insert` 或 `stream load` 时填充默认值。

## 语法

```Haskell
PERCENTILE_EMPTY();
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 PERCENTILE。

## 示例

建表。

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
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);
```

插入数据。

```sql
insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_empty());
```
