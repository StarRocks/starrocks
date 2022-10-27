# 动态分区

## 创建支持动态分区的表

如下示例，创建一张支持动态分区的表，表名为 `site_access`，动态分区通过 `PEROPERTIES` 进行配置。分区的区间为当前时间的前后 3 天，总共 6 天。

```SQL
CREATE TABLE site_access(
event_day DATE,
site_id INT DEFAULT '10',
city_code VARCHAR(100),
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(
PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
)
DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32
PROPERTIES(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32"
);
```

`PROPERTIES` 配置项如下：

- `dynamic_partition.enable`：是否开启动态分区特性，取值为 `TRUE` 或 `FALSE`。默认值为 `TRUE`。

- `dynamic_partition.time_unit`：必填，调度动态分区特性的时间粒度，取值为 `DAY`、`WEEK` 或 `MONTH`，表示按天、周或月调度动态分区特性。并且，时间粒度必须对应分区名后缀格式。具体对应规则如下：
  - 取值为 `DAY` 时，分区名后缀的格式应该为 yyyyMMdd，例如 `20200321`。
  
    ```SQL

    PARTITION BY RANGE(event_day)(
    PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
    PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
    PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
    PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
    )
    ```

  - 取值为 `WEEK` 时，分区名后缀的格式应该为 yyyy_ww，例如 `2020_13` 代表 2020 年第 13 周。
  - 取值为 `MONTH` 时，分区名后缀的格式应该为 yyyyMM，例如 `202003`。

- `dynamic_partition.start`：必填，动态分区的开始时间。以当天为基准，超过该时间范围的分区将会被删除。取值范围为小于 0 的负整数，最大值为 **-1**。默认值为 **Integer.MIN_VALUE**，即 -2147483648。

- `dynamic_partition.end`：必填，动态分区的结束时间。 以当天为基准，提前创建指定数量的分区。取值范围为大于 0 的正整数，最小值为 **1**。

- `dynamic_partition.prefix`: 动态分区的前缀名，默认值为 **p**。

- `dynamic_partition.buckets`: 动态分区的分桶数量，默认与 BUCKETS 关键词指定的分桶数量保持一致。

## 查看表当前的分区情况

开启动态分区特性后，会不断地自动增减分区。您可以执行如下语句，查看表当前的分区情况：

```SQL
SHOW PARTITIONS FROM site_access;
```

假设当前时间为 2020-03-25，在调度动态分区时，会删除分区上界小于 2020-03-22 的分区，同时在调度时会创建今后 3 天的分区。则如上语句的返回结果中，`Range` 列显示当前分区的信息如下：

```SQL
[types: [DATE]; keys: [2020-03-22]; ‥types: [DATE]; keys: [2020-03-23]; )
[types: [DATE]; keys: [2020-03-23]; ‥types: [DATE]; keys: [2020-03-24]; )
[types: [DATE]; keys: [2020-03-24]; ‥types: [DATE]; keys: [2020-03-25]; )
[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

## 修改表的动态分区属性

执行 ALTER TABLE，修改动态分区的属性，例如暂停或者开启动态分区特性。

```SQL
ALTER TABLE site_access SET("dynamic_partition.enable"="false");
ALTER TABLE site_access SET("dynamic_partition.enable"="true");
```

> 说明：
>
> - 可以执行 SHOW CREATE TABLE 命令，查看表的动态分区属性。
>
> - ALTER TABLE 也适用于修改 `PEROPERTIES` 中的其他配置项。

## 使用说明

开启动态分区特性，相当于将创建分区的判断逻辑交由 StarRocks 完成。因此创建表时，必须保证动态分区配置项 `dynamic_partition.time_unit` 指定的时间粒度与分区名后缀格式对应，否则创建表会失败。具体对应规则如下：

- `dynamic_partition.time_unit` 指定为 `DAY` 时，分区名后缀的格式应该为 yyyyMMdd，例如 `20200325`。

```SQL
PARTITION BY RANGE(event_day)(
PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
)
```

- `dynamic_partition.time_unit`指定为 `WEEK` 时，分区名后缀的格式应该为 yyyy_ww，例如 `2020_13`，代表 2020 年第 13 周。

- `dynamic_partition.time_unit`指定为 `MONTH` 时，分区名后缀的格式应该为 yyyyMM，例如 `202003`。
