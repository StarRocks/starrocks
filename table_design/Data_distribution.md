# 数据分布

建表时，您需要通过设置分区和分桶，指定数据分布方式，并且建议您合理设置分区和分桶，实现数据均匀的分布。数据分布是指数据划分为子集，并按一定规则均衡地分布在不同节点上，能够有效裁剪数据扫描量，最大限度地利用集群的并发性能，从而提升查询性能。

## 数据分布概览

### 常见的数据分布方式

现代分布式数据库中，常见的数据分布方式有如下四种：Round-Robin、Range、List 和 Hash。如下图所示:

![数据分布方式](../assets/3.3.2-1.png)

- Round-Robin：以轮询的方式把数据逐个放置在相邻节点上。

- Range：按区间进行数据分布。如上图所示，区间 [1-3]、[4-6] 分别对应不同的范围 (Range)。

- List：直接基于离散的各个取值做数据分布，性别、省份等数据就满足这种离散的特性。每个离散值会映射到一个节点上，多个不同的取值可能也会映射到相同节点上。

- Hash：通过哈希函数把数据映射到不同节点上。

为了更灵活地划分数据，除了单独采用上述四种数据分布方式之一以外，您还可以根据具体的业务场景需求组合使用这些数据分布方式。常见的组合方式有 Hash+Hash、Range+Hash、Hash+List。

### StarRocks 的数据分布方式

StarRocks 支持如下两种数据分布方式：

- Hash 数据分布方式：一张表为一个分区，分区按照分桶键和分桶数量进一步进行数据划分。

- Range+Hash 数据分布方式：一张表拆分成多个分区，每个分区按照分桶键和分桶数量进一步进行数据划分。

采用 Hash 分布的建表语句如下，其中分桶键为 `site_id`：

```SQL
CREATE TABLE site_access(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

采用Range+Hash组合分布的建表语句如下，其中分区键为 `event_day`，分桶键为 `site_id`：

```SQL
CREATE TABLE site_access(
event_day DATE,
site_id INT DEFAULT '10',
city_code VARCHAR(100),
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)
(
PARTITION p1 VALUES LESS THAN ("2020-01-31"),
PARTITION p2 VALUES LESS THAN ("2020-02-29"),
PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

#### 分区

分区⽤于将数据划分成不同的区间。分区的主要作⽤是将⼀张表按照分区键拆分成不同的管理单元，针对每⼀个管理单元选择相应的存储策略，⽐如副本数、冷热策略和存储介质等等。StarRocks 支持在一个集群内使用多种存储介质，您可以将新数据所在分区放在 SSD 盘上，利用 SSD 的优秀的随机读写性能来提高查询性能，将旧数据存放在 SATA 盘上，以节省数据存储的成本。

业务系统中⼀般会选择根据时间进⾏分区，以优化⼤量删除过期数据带来的性能问题，同时也⽅便冷热数据分级存储。

StarRocks 支持动态分区。您可以按需为新数据动态创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的实效性，实现对分区的⽣命周期管理（Time to Life，简称 “TTL”），⼤幅减少运维管理的成本。

StarRocks 还支持批量创建分区。

#### 分桶

分区的下⼀级是分桶，StarRocks 采⽤ Hash 算法作为分桶算法。在同⼀分区内，分桶键哈希值相同的数据形成 Tablet，Tablet 以多副本冗余的形式存储，是数据均衡和恢复的最⼩单位。Tablet 的副本由⼀个单独的本地存储引擎管理，数据导⼊和查询最终都下沉到所涉及的 Tablet 副本上。
建表时，必须指定分桶键。

## 设置分区和分桶

### 选择分区键

选择合理的分区键可以有效的裁剪扫描的数据量。**目前仅支持分区键的数据类型为日期和整数类型。**在实际业务场景中，一般从数据管理的角度选择分区键，常见的分区键为时间或者区域。按照分区键划分数据后，单个分区原始数据量建议不要超过 100 GB。

### 选择分桶键

选择高基数的列（例如唯一 ID）来作为分桶键，可以保证数据在各个分桶中尽可能均衡。如果数据倾斜情况严重，您可以使用多个列作为数据的分桶键，但是不建议超过 3 个列。

还是以上述 Range+Hash 组合分布的建表语句为例：

```SQL
CREATE TABLE site_access(
event_day DATE,
site_id INT DEFAULT '10',
city_code VARCHAR(100),
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)
(
PARTITION p1 VALUES LESS THAN ("2020-01-31"),
PARTITION p2 VALUES LESS THAN ("2020-02-29"),
PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

如上示例中，`site_access` 表采用 `site_id` 作为分桶键，其原因在于，针对 `site_access` 表的查询请求，基本上都以站点（高基数列）作为查询过滤条件。采用 `site_id` 作为分桶键，可以在查询时裁剪掉大量无关分桶。

如下查询中，10 个分桶中的 9 个分桶被裁减，因而系统只需要扫描 `site_access` 表中 1/10 的数据：

```SQL
select sum(pv)
from site_access
where site_id = 54321;
```

但是如果 `site_id` 分布十分不均匀，大量的访问数据是关于少数网站的（幂律分布, 二八规则），那么采用上述分桶方式会造成数据分布出现严重的倾斜，进而导致系统局部的性能瓶颈。此时，您需要适当调整分桶的字段，以将数据打散，避免性能问题。例如，可以采用 `site_id`、`city_code` 组合作为分桶键，将数据划分得更加均匀。相关建表语句如下：

```SQL
CREATE TABLE site_access
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id,city_code) BUCKETS 10;
```

在实际使用中，您可以依据自身的业务特点选择以上两种分桶方式。采用 `site_id` 的分桶方式对于短查询十分有利，能够减少节点之间的数据交换，提高集群整体性能；采用 `site_id`、`city_code` 的组合分桶方式对于长查询有利，能够利用分布式集群的整体并发性能。

> 说明：
>
> - 短查询是指扫描数据量不大、单机就能完成扫描的查询。
>
> - 长查询是指扫描数据量大、多机并行扫描能显著提升性能的查询。

### 确定分桶数量

在 StarRocks 中，分桶是实际物理文件组织的单元。自 2.4 版本起，StarRocks 提供了自适应的 Tablet 并行扫描能力，即一个查询中涉及到的任意一个 Tablet 可能是由多个线程并行地分段扫描，减少了 Tablet 数量对查询能力的限制，从而可以简化对分桶数量的设定。简化后的分桶方式可以是：首先预估每个分区的数据量，然后按照每 10 GB 原始数据一个 Tablet 计算，从而确定分桶数量。

> 注意：
>
> - 您需要执行`SET GLOBAL enable_tablet_internal_parallel;`，开启并行扫描 Tablet。
>
> - 不支持修改已创建的分区的分桶数量，支持在增加分区时为新增分区设置新的分桶数量。

### 管理分区

#### 增加分区

增加新的分区，用于存储新的数据。新增分区的默认分桶数量和原分区相同。您也可以根据新分区的数据规模调整分桶数量。

如下示例中，在 `site_access` 表添加新的分区，用于存储新月份的数据，并且调整分桶数量为 20：

```SQL
ALTER TABLE site_access
ADD PARTITION p4 VALUES LESS THAN ("2020-04-30")
DISTRIBUTED BY HASH(site_id) BUCKETS 20;
```

#### 删除分区

执行如下语句，删除 `site_access` 表中分区 p1 及数据：

> 说明：分区中的数据不会立即删除，会在 Trash 中保留一段时间（默认为一天）。如果误删分区，可以通过 RECOVER 命令恢复分区及数据。

```SQL
ALTER TABLE site_access
DROP PARTITION p1;
```

#### 恢复分区

执行如下语句，恢复 `site_access` 表中分区 `p1` 及数据：

```SQL
RECOVER PARTITION p1 FROM site_access;
```

#### 查看分区

执行如下语句，查看 `site_access` 表中分区情况：

```SQL
SHOW PARTITIONS FROM site_access;
```

### 最佳实践

对于 StarRocks 而言，分区和分桶的选择是非常关键的。在建表时选择合理的分区键和分桶键，可以有效提高集群整体性能。因此建议在选择分区键和分桶键时，根据业务情况进行调整。

- 数据倾斜
  - 如果业务场景中单独采用倾斜度大的列做分桶，很大程度会导致访问数据倾斜，那么建议采用多列组合的方式进行数据分桶。

- 高并发
  - 分区和分桶应该尽量覆盖查询语句所带的条件，这样可以有效减少扫描数据，提高并发。

- 高吞吐
  - 尽量把数据打散，让集群以更高的并发扫描数据，完成相应计算。

## 管理动态分区

### 创建支持动态分区的表

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

### 查看表当前的分区情况

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

### 修改表的动态分区属性

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

### 使用说明

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

## 批量创建分区

> StarRocks 1.16 及以后支持该功能。

建表时和建表后，支持批量创建分区，通过 START、END 指定批量分区的开始和结束，EVERY 子句指定分区增量值。其中，批量分区包含 START 的值，但是不包含 END 的值。分区的命名规则同动态分区一样。

### 建表时批量创建日期分区

当分区键为日期类型时，建表时通过 START、END 指定批量分区的开始日期和结束日期，EVERY 子句指定分区增量值。并且 EVERY 子句中用 INTERVAL 关键字表示日期间隔，目前仅支持日期间隔的单位为 DAY、WEEK、MONTH、YEAR。

如下示例中，批量分区的开始日期为 `2021-01-01` 和结束日期为 `2021-01-04`，增量值为一天：

```SQL
CREATE TABLE site_access (
    datekey DATE,
    site_id INT,
    city_code SMALLINT,
    user_name VARCHAR(32),
    pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (
    START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1" 
);
```

则相当于在建表语句中使用如下 PARTITION BY 子句：

```SQL
PARTITION BY RANGE (datekey) (
PARTITION p20210101 VALUES [('2021-01-01'), ('2021-01-02')),
PARTITION p20210102 VALUES [('2021-01-02'), ('2021-01-03')),
PARTITION p20210103 VALUES [('2021-01-03'), ('2021-01-04'))
)
```

### 建表时批量创建不同日期间隔的日期分区

建表时批量创建日期分区时，支持针对不同的日期分区区间（日期分区区间不能相重合），使用不同的 EVERY 子句指定日期间隔。一个日期分区区间，按照对应 EVERY 子句定义的日期间隔，批量创建分区，例如：

```SQL
CREATE TABLE site_access (
    datekey DATE,
    site_id INT,
    city_code SMALLINT,
    user_name VARCHAR(32),
    pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (
    START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR),
    START ("2021-01-01") END ("2021-05-01") EVERY (INTERVAL 1 MONTH),
    START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
```

则相当于在建表语句中使用如下 PARTITION BY 子句：

```SQL
PARTITION BY RANGE (datekey) (
PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01')),
PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
PARTITION p20210501 VALUES [('2021-05-01'), ('2021-05-02')),
PARTITION p20210502 VALUES [('2021-05-02'), ('2021-05-03')),
PARTITION p20210503 VALUES [('2021-05-03'), ('2021-05-04'))
)
```

### 建表时批量创建数字分区

当分区键为整数类型时，建表时通过 START、END 指定批量分区的开始值和结束值，EVERY 子句指定分区增量值。

> 说明：分区键的值需要使用英文引号包裹，而 EVERY 子句中的分区增量值不用英文引号包裹。

如下示例中，批量分区的开始值为 `1` 和结束值为 `5`，分区增量值为 `1`：

```SQL
CREATE TABLE site_access (
    datekey INT,
    site_id INT,
    city_code SMALLINT,
    user_name VARCHAR(32),
    pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (
    START ("1") END ("5") EVERY (1)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
```

则相当于在建表语句中使用如下 PARTITION BY 子句：

```SQL
PARTITION BY RANGE (datekey) (
PARTITION p1 VALUES [("1"), ("2")),
PARTITION p2 VALUES [("2"), ("3")),
PARTITION p3 VALUES [("3"), ("4")),
PARTITION p4 VALUES [("4"), ("5"))
)
```

### 建表后批量创建分区

建表后，支持通过ALTER TABLE 语句批量创建分区。相关语法与建表时批量创建分区类似，通过指定 ADD PARTITIONS 关键字，以及 START、END 以及 EVERY 子句来批量创建分区。示例如下：

```SQL
ALTER TABLE site_access 
ADD PARTITIONS START ("2021-01-04") END ("2021-01-06") EVERY (INTERVAL 1 DAY);
```
