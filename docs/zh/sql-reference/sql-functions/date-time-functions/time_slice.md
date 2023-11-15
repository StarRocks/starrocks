# time_slice

## 功能

根据指定的时间粒度周期，将给定的时间转化为其所在的时间粒度周期的起始或结束时刻。

该函数从 2.3 版本开始支持。从 2.5 版本开始支持转化为结束时刻。

## 语法

```Haskell
DATETIME time_slice(DATETIME dt, INTERVAL N type[, boundary])
```

## 参数说明

- `dt`：需要转化的时间。支持的数据类型为 DATETIME。

- `INTERVAL N type`：时间粒度周期，例如 `interval 5 second` 表示时间粒度为 5 秒。
  - `N` 是 INT 类型的时间粒度周期的长度。
  - `type` 是时间粒度周期的单位，取值可以是 YEAR，QUARTER，MONTH，WEEK，DAY，HOUR，MINUTE，SECOND。

- `boundary`：可选，用于指定返回时间周期的起始时刻 (`FLOOR`) 还是结束时刻 (`CEIL`)。取值范围：FLOOR，CEIL。如果不指定，默认为 `FLOOR`。该参数从 2.5 版本开始支持。

## 返回值说明

返回值的数据类型为 DATETIME。

## 注意事项

时间粒度周期从公元 `0001-01-01 00:00:00` 算起。

## 示例

假设有表 `test_all_type_select`，数据以 `id_int` 排序：

```Plaintext
select * from test_all_type_select order by id_int;

+------------+---------------------+--------+
| id_date    | id_datetime         | id_int |
+------------+---------------------+--------+
| 2052-12-26 | 1691-12-23 04:01:09 |      0 |
| 2168-08-05 | 2169-12-18 15:44:31 |      1 |
| 1737-02-06 | 1840-11-23 13:09:50 |      2 |
| 2245-10-01 | 1751-03-21 00:19:04 |      3 |
| 1889-10-27 | 1861-09-12 13:28:18 |      4 |
+------------+---------------------+--------+
5 rows in set (0.06 sec)
```

示例一：将给定的 DATETIME 时间 `id_datetime` 转化为以 5 秒为时间粒度周期的起始时刻 （不指定 `boundary` 参数，默认为 `FLOOR`）。

```Plaintext
select time_slice(id_datetime, interval 5 second)
from test_all_type_select
order by id_int;

+---------------------------------------------------+
| time_slice(id_datetime, INTERVAL 5 second, floor) |
+---------------------------------------------------+
| 1691-12-23 04:01:05                               |
| 2169-12-18 15:44:30                               |
| 1840-11-23 13:09:50                               |
| 1751-03-21 00:19:00                               |
| 1861-09-12 13:28:15                               |
+---------------------------------------------------+
5 rows in set (0.16 sec)
```

示例二：将给定的 DATETIME 时间 `id_datetime` 转化为以 5 天为时间粒度周期的起始时刻（设置`boundary` 为 `FLOOR`）。

```Plaintext
select time_slice(id_datetime, interval 5 day, FLOOR)
from test_all_type_select
order by id_int;

+------------------------------------------------+
| time_slice(id_datetime, INTERVAL 5 day, floor) |
+------------------------------------------------+
| 1691-12-22 00:00:00                            |
| 2169-12-16 00:00:00                            |
| 1840-11-21 00:00:00                            |
| 1751-03-18 00:00:00                            |
| 1861-09-12 00:00:00                            |
+------------------------------------------------+
5 rows in set (0.15 sec)
```

示例三：将给定的 DATETIME 时间 `id_datetime` 转化为以 5 天为时间粒度周期的结束时刻。

```Plaintext
select time_slice(id_datetime, interval 5 day, CEIL)
from test_all_type_select
order by id_int;

+-----------------------------------------------+
| time_slice(id_datetime, INTERVAL 5 day, ceil) |
+-----------------------------------------------+
| 1691-12-27 00:00:00                           |
| 2169-12-21 00:00:00                           |
| 1840-11-26 00:00:00                           |
| 1751-03-23 00:00:00                           |
| 1861-09-17 00:00:00                           |
+-----------------------------------------------+
5 rows in set (0.12 sec)
```
