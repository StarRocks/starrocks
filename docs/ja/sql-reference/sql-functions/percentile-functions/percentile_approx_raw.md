---
displayed_sidebar: docs
---

# percentile_approx_raw

`x` から指定されたパーセンタイルに対応する値を返します。

`x` が列の場合、この関数はまず `x` の値を昇順にソートし、パーセンタイル `y` に対応する値を返します。

## Syntax

```Haskell
PERCENTILE_APPROX_RAW(x, y);
```

## Parameters

- `x`: 列または値の集合であることができます。PERCENTILE に評価される必要があります。

- `y`: パーセンタイル。サポートされているデータ型は DOUBLE です。値の範囲: [0.0,1.0]。

## Return value

PERCENTILE 値を返します。

## Examples

`percent` 列が percentile_approx_raw() の入力である `aggregate_tbl` テーブルを作成します。

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
  PROPERTIES ("replication_num" = "1");
  ```

テーブルにデータを挿入します。

  ```sql
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(1));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(2));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(3));
  insert into aggregate_tbl values (5, '2020-02-23', 'city_code', 555, percentile_hash(4));
  ```

パーセンタイル 0.5 に対応する値を計算します。

  ```Plain Text
  mysql> select percentile_approx_raw(percent, 0.5) from aggregate_tbl;
  +-------------------------------------+
  | percentile_approx_raw(percent, 0.5) |
  +-------------------------------------+
  |                                 2.5 |
  +-------------------------------------+
  1 row in set (0.03 sec)
  ```