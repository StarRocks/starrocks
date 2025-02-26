---
displayed_sidebar: docs
---

# percentile_empty

PERCENTILE 値を構築します。これは、[Stream Load](../../../loading/StreamLoad.md) または [INSERT INTO](../../../loading/InsertInto.md) を使用してデータロードのために null 値を埋めるために使用されます。

## Syntax

```Haskell
PERCENTILE_EMPTY();
```

## Parameters

なし

## Return value

PERCENTILE 値を返します。

## Examples

テーブルを作成します。`percent` 列は PERCENTILE 列です。

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

テーブルにデータを挿入します。

```sql
INSERT INTO aggregate_tbl VALUES
(5, '2020-02-23', 'city_code', 555, percentile_empty());
```