# date_slice

## 功能

根据指定的时间粒度周期，将给定的时间转化到其所在的时间粒度周期的起始或结束时刻。

该函数从 2.5 版本开始支持。

## 语法

```Haskell
DATE date_slice(DATE dt, INTERVAL N type[, boundary])
```

## 参数说明

- `dt`：需要转化的时间。支持的数据类型为 DATE。
- `INTERVAL N type`：时间粒度周期，例如 `interval 5 day` 表示时间粒度为 5 天。
  - `N` 是 INT 类型的时间周期的长度。
  - `type` 是时间粒度周期的单位，取值可以是 YEAR，QUARTER，MONTH，WEEK，DAY。 对于 DATE 类型的输入值，`type` 不能为时分秒，否则返回报错。
- `boundary`：可选，用于指定返回时间周期的起始时刻 (FLOOR) 还是结束时刻 (CEIL)。取值范围：FLOOR，CEIL。如果不指定，默认为 FLOOR。

## 返回值说明

返回值的数据类型为 DATE。

## 注意事项

时间粒度周期从公元`0001-01-01`算起。

## 示例

示例一：将给定时间转化到以 5 年为时间粒度周期的起始时刻（不指定 `boundary` 参数，默认为 `FLOOR`）。

```Plaintext
select date_slice('2022-04-26', interval 5 year);
+--------------------------------------------------+
| date_slice('2022-04-26', INTERVAL 5 year, floor) |
+--------------------------------------------------+
| 2021-01-01                                       |
+--------------------------------------------------+
```

示例二：将给定时间转化到以 5 天为时间粒度周期的结束时刻。

```Plaintext
select date_slice('0001-01-07', interval 5 day, CEIL);
+------------------------------------------------+
| date_slice('0001-01-07', INTERVAL 5 day, ceil) |
+------------------------------------------------+
| 0001-01-11                                     |
+------------------------------------------------+
```

以下示例使用表 `test_all_type_select`，数据以 `id_int` 排序：

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

示例三：将给定时间 `id_date` 转化为以 5 秒为时间粒度周期的起始时刻。

```Plaintext
select date_slice(id_date, interval 5 second, FLOOR)
from test_all_type_select
order by id_int;
ERROR 1064 (HY000): can't use date_slice for date with time(hour/minute/second)
```

返回报错，提示对于 DATE 类型的时间值不能使用时间部分(时/分/秒)。

示例四：将给定时间 `id_date` 转化到以 5 天为时间粒度周期的起始时刻。

```Plaintext
select date_slice(id_date, interval 5 day, FLOOR)
from test_all_type_select
order by id_int;
+--------------------------------------------+
| date_slice(id_date, INTERVAL 5 day, floor) |
+--------------------------------------------+
| 2052-12-24                                 |
| 2168-08-03                                 |
| 1737-02-04                                 |
| 2245-09-29                                 |
| 1889-10-25                                 |
+--------------------------------------------+
5 rows in set (0.14 sec)
```

示例五：将给定时间 `id_date` 转化到以 5 天为时间粒度周期的结束时刻。

```Plaintext
select date_slice(id_date, interval 5 day, CEIL)
from test_all_type_select
order by id_int;
+-------------------------------------------+
| date_slice(id_date, INTERVAL 5 day, ceil) |
+-------------------------------------------+
| 2052-12-29                                |
| 2168-08-08                                |
| 1737-02-09                                |
| 2245-10-04                                |
| 1889-10-30                                |
+-------------------------------------------+
5 rows in set (0.17 sec)
```
