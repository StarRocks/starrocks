# 动态分区

动态分区功能开启后，您可以按需为新数据动态地创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的时效性。

## 创建支持动态分区的表

如下示例，创建一张支持动态分区的表，表名为 `site_access`，动态分区通过 `PROPERTIES` 进行配置。分区的区间为当前时间的前后 3 天，总共 6 天。

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
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.history_partition_num" = "0"
);
```

 **动态分区相关属性 `PROPERTIES`：**

| 参数                      | 是否必填 | 说明                                                                                                                                                                                                                                                                                                                    |
| ----------------------- |-----|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dynamic_partition.enable | 否   | 开启动态分区特性，取值为 `true`（默认）或 `false`。                                                                                                                                                                                                                                                                                     |
| dynamic_partition.time_unit | 是   | 动态分区的时间粒度，取值为 `HOUR`、`DAY`、`WEEK` 或 `MONTH`。时间粒度会决定动态创建的分区名后缀格式。  <br>取值为 `HOUR` 时，仅支持Datetime类型，动态创建的分区名后缀格式为 yyyyMMddHH，例如 `2020032101`。<br>取值为 `DAY` 时，动态创建的分区名后缀格式为 yyyyMMdd，例如 `20200321`。<br>取值为 `WEEK` 时，动态创建的分区名后缀格式为 yyyy_ww，例如 `2020_13` 代表 2020 年第 13 周。<br>取值为 `MONTH` 时，动态创建的分区名后缀格式为 yyyyMM，例如 `202003`。<br>**注意**<br>动态分区不支持时间粒度为 `YEAR`，手动和自动创建分区支持时间粒度为 `YEAR`。 |
| dynamic_partition.time_zone |  否   | 动态分区的时区。默认与系统时区一致。                                                                                                                                                                                                                                                                                            |
| dynamic_partition.start | 否   | 保留的动态分区的起始偏移，取值范围为负整数。根据 `dynamic_partition.time_unit` 属性的不同，以当天（周/月）为基准，分区范围在此偏移之前的分区将会被删除。比如设置为`-3`，并且`dynamic_partition.time_unit`为`day`，则表示 3 天前的分区会被删掉。<br>如果不填写，则默认为 `Integer.MIN_VALUE`，即 `-2147483648`，表示不删除历史分区。                                                                                             |
| dynamic_partition.end   | 是   | 提前创建的分区数量，取值范围为正整数。根据 `dynamic_partition.time_unit` 属性的不同，以当天（周/月）为基准，提前创建对应范围的分区。                                                                                                                                                                                                                                    |
| dynamic_partition.prefix | 否   | 动态分区的前缀名，默认值为 `p`。                                                                                                                                                                                                                                                                                                    |
| dynamic_partition.buckets | 否   | 动态分区的分桶数量。默认与 BUCKETS 保留字指定的分桶数量、或者 StarRocks 自动设置的分桶数量保持一致。                                                                                                                                                                                                                                                          |
| dynamic_partition.history_partition_num | 否   | 动态分区的创建历史分区的个数，默认值为 `0`。当值>0时会提前创建历史分区。自 2.5.2 版本起，StarRocks 支持配置该参数。                                                                                                                                                                                                                                                                              |
| dynamic_partition.start_day_of_week     | 否   |      当 `dynamic_partition.time_unit` 为 `WEEK` 时，该参数指定每周的第一天。有效值为 `1` 到 `7`。`1` 表示星期一，`7` 表示星期日。默认值为 `1`，表示每周从星期一开始。  |
| dynamic_partition.start_day_of_month     | 否   |     当 `dynamic_partition.time_unit` 为 `MONTH` 时，该参数指定每月的第一天。有效值为 `1` 到 `28`。`1` 表示每月的第一天，`28` 表示每月的第 `28` 天。默认值为 `1`，表示每月从第一天开始。每月的第一天不支持为 29 日、30 日或 31 日。   |
| dynamic_partition.replication_num     | 否   |  在动态创建的分区中，每个 tablet 副本的数量。默认值与建表时配置的副本数量相同。      |

**动态分区相关 FE 配置项：**

`dynamic_partition_check_interval_seconds`：FE 配置项，动态分区检查的时间周期，默认为 600，单位为 s，即每10分钟检查一次分区情况是否满足`PROPERTIES`中动态分区属性，如不满足，则会自动创建和删除分区。

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
> - ALTER TABLE 也适用于修改 `PROPERTIES` 中的其他配置项。
