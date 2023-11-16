---
displayed_sidebar: "English"
---

# percentile_empty

## Description

Constructs a PERCENTILE value, which is used to fill in null values for data loading using [Stream Load](../../../loading/StreamLoad.md) or [INSERT INTO](../../../loading/InsertInto.md).

## Syntax

```Haskell
PERCENTILE_EMPTY();
```

## Parameters

None

## Return value

Returns a PERCENTILE value.

## Examples

Create a table. The `percent` column is a PERCENTILE column.

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
PROPERTIES ("replication_num" = "3");
```

Insert data into the table.

```sql
INSERT INTO aggregate_tbl VALUES
(5, '2020-02-23', 'city_code', 555, percentile_empty());
```
