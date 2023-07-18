# 数据分布

建表时，您可以通过设置合理的分区和分桶，实现数据均匀分布和查询性能提升。数据均匀分布是指数据按照一定规则划分为子集，并且均衡地分布在不同节点上。查询时能够有效裁剪数据扫描量，最大限度地利用集群的并发性能，从而提升查询性能。

> **说明**
>
> - 自 3.1 版本起，您在建表和新增分区时可以不设置分桶键。StarRocks 默认使用随机分桶，将数据随机地分布在分区的所有分桶中。更多信息，请参见[随机分桶](#随机分桶自-v31)。
> - 自 2.5.7 版本起，您在建表和新增分区时无需手动设置分桶数量 (BUCKETS)。StarRocks 默认自动设置分桶数量，如果自动设置分桶数量后性能未能达到预期，并且您比较熟悉分桶机制，则您也可以[手动设置分桶数量](#确定分桶数量)。

## 数据分布概览

### 常见的数据分布方式

现代分布式数据库中，常见的数据分布方式有如下几种：Round-Robin、Range、List 和 Hash。如下图所示：

![数据分布方式](../assets/3.3.2-1.png)

- Round-Robin：以轮询的方式把数据逐个放置在相邻节点上。

- Range：按区间进行数据分布。如上图所示，区间 [1-3]、[4-6] 分别对应不同的范围 (Range)。

- List：直接基于离散的各个取值做数据分布，性别、省份等数据就满足这种离散的特性。每个离散值会映射到一个节点上，多个不同的取值可能也会映射到相同节点上。

- Hash：通过哈希函数把数据映射到不同节点上。

为了更灵活地划分数据，除了单独采用上述数据分布方式之一以外，您还可以根据具体的业务场景需求组合使用这些数据分布方式。常见的组合方式有 Hash+Hash、Range+Hash、Hash+List。

### StarRocks 的数据分布方式

StarRocks 采用分区 + 分桶的两层分布方式来灵活地划分数据。

- 第一层为分区：支持 Range 分布，或者不分区（即全表只有一个分区）。
- 第二层为分桶：在一个分区中，必须进行分桶。数据分布至分桶的方式有 Hash 和 Random 两种。分桶的数量可以由系统自动设置（推荐）或者由您手动指定。

> **说明**
>
> 除了常见的分布方式外， StarRocks 还支持了 Random 分布，简化分桶设置。

综上所述，StarRocks 支持 Random、Hash、Range + Random、Range + Hash 4 种数据分布方式。

- Random 分布：一张表为一个分区，表中数据随机分布至不同分桶，适用于数据量不大且随时间增长缓慢的场景。**如果您没有设置数据分布方式，则 StarRocks 默认使用此数据分布方式。**

- Hash 分布：一张表为一个分区，表中数据按照分桶键值和分桶数量进行划分。

- Range + Random 分布：一张表的数据按照分区列值的范围划分成不同分区。同一分区的数据随机分布至不同分桶。

- Range + Hash 分布：一张表的数据按照分区列值的范围划分成不同分区。每个分区的数据按照分桶键值和分桶数量进一步进行划分。

例如，建表时不设置数据分布方式，则默认由 StarRocks 使用 Random 分布：

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10', 
    pv BIGINT DEFAULT '0' ,
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT ''
)
DUPLICATE KEY(event_day,site_id,pv); -- 没有设置分布方式
```

建表时设置数据分布方式为 Hash 分布：

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
DISTRIBUTED BY HASH(event_day,site_id); -- 设置分桶方式为 Hash 分桶，并且必须指定分桶键
```

建表时设置数据分布方式为 Range + Random 分布：

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10', 
    pv BIGINT DEFAULT '0' ,
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT ''
)
DUPLICATE KEY(event_day,site_id,pv)
-- 设置分区方式为 Range 分区
PARTITION BY RANGE(event_day) (
    PARTITION p1 VALUES LESS THAN ("2020-01-31"),
    PARTITION p2 VALUES LESS THAN ("2020-02-29"),
    PARTITION p3 VALUES LESS THAN ("2020-03-31")
); -- 没有设置分桶方式，默认由 StarRocks 使用 Random 分桶
```

建表时设置数据分布方式为 Range + Hash 分布：

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
-- 设置分区方式为 Range 分区
PARTITION BY RANGE(event_day) (
    PARTITION p1 VALUES LESS THAN ("2020-01-31"),
    PARTITION p2 VALUES LESS THAN ("2020-02-29"),
    PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
-- 设置分桶方式为 Hash 分桶，必须指定分桶键
DISTRIBUTED BY HASH(event_day, site_id);
```

#### 分区

分区用于将数据划分成不同的区间。分区的主要作用是将一张表按照分区键拆分成不同的管理单元，针对每一个管理单元选择相应的存储策略，比如分桶数、冷热策略、存储介质、副本数等。StarRocks 支持在一个集群内使用多种存储介质，您可以将新数据所在分区放在 SSD 盘上，利用 SSD 优秀的随机读写性能来提高查询性能，将旧数据存放在 SATA 盘上，以节省数据存储的成本。

业务系统中⼀般会选择根据时间进行分区，以优化大量删除过期数据带来的性能问题，同时也方便冷热数据分级存储。

自 3.0 版本起，StarRocks 支持导入时自动创建分区，您不再需要提前创建出大量分区。这种按需创建分区的方式可以帮助您减少运维管理的成本。

如果您确实需要提前创建分区，则可以使用 StarRocks 其他分区创建方式：动态创建分区、手动创建分区和批量创建分区。

分区单位的选择，需要综合考虑数据量、查询特点、数据管理粒度等因素。

示例 1: 表单月数据量很小，可以按月分区，相比于按天分区，可以减少元数据数量，从而减少元数据管理和调度的资源消耗。

示例 2: 表单月数据量很大，而大部分查询条件精确到天，如果按天分区，可以做有效的分区裁减，减少查询扫描的数据量。

示例 3: 数据要求按天过期，可以按天分区。

#### 分桶

一个分区按分桶方式被分成了多个桶 bucket，每个桶的数据称之为一个 tablet。

分桶方式：StarRocks 支持[随机分桶](#随机分桶自-v31)（自 3.1 版本起） 和 [哈希分桶](#哈希分桶)。

- 随机分桶，建表和新增分区时无需设置分桶键。在同一分区内，数据随机分布到不同的分桶中。
- 哈希分桶，建表和新增分区时需要指定分桶键。在同一分区内，数据按照分桶键划分分桶后，所有分桶键的值相同的行会唯一分配到对应的一个分桶。

分桶数量：默认由 StarRocks 自动设置分桶数量（自 v2.5.7 版本）。同时也支持您手动设置分桶数量。更多信息，请参见[确定分桶数量](#确定分桶数量)。

## 创建和管理分区

### 自动创建分区

> 注意
>
> StarRocks 存算分离模式暂时不支持该功能。
自 3.0 版本起，StarRocks 支持导入时自动创建分区。相比于其他创建分区的方式，这种创建分区的方式更加易用灵活，因此推荐您优先使用该方式。您只需要在建表时在包含时间函数的分区表达式中，指定一个 DATE 或者 DATETIME 类型的分区列，以及指定分区粒度。借助这种使用表达式的隐式分区方式，StarRocks 会写入新数据时自动创建对应分区，您不需要预先创建出大量分区。

### 动态分区

您可以按需为新数据[动态创建分区](./dynamic_partitioning.md)，同时 StarRocks 会⾃动删除过期分区，从而确保数据的实效性，实现对分区的⽣命周期管理（Time to Life，简称 “TTL”）。

### 手动创建分区

选择合理的分区键可以有效的裁剪扫描的数据量。**目前仅支持分区键的数据类型为日期和整数类型**。在实际业务场景中，一般从数据管理的角度选择分区键，常见的分区键为时间或者区域。

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(
    PARTITION p1 VALUES LESS THAN ("2020-01-31"),
    PARTITION p2 VALUES LESS THAN ("2020-02-29"),
    PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
DISTRIBUTED BY HASH(site_id);
```

### 批量创建分区

> StarRocks 1.16 及以后版本支持该功能。

建表时和建表后，支持批量创建分区，通过 START、END 指定批量分区的开始和结束，EVERY 子句指定分区增量值。其中，批量分区包含 START 的值，但是不包含 END 的值。分区的命名规则同动态分区一样。

**建表时批量创建日期分区**

当分区键为日期类型时，建表时通过 START、END 指定批量分区的开始日期和结束日期，EVERY 子句指定分区增量值。并且 EVERY 子句中用 INTERVAL 关键字表示日期间隔，目前支持日期间隔的单位为 HOUR（自 3.0 版本起）、DAY、WEEK、MONTH、YEAR。

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
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
    "replication_num" = "3" 
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

**建表时批量创建不同日期间隔的日期分区**

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
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
    "replication_num" = "3"
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

**建表时批量创建数字分区**

当分区键为整数类型时，建表时通过 START、END 指定批量分区的开始值和结束值，EVERY 子句指定分区增量值。

> 说明：START、END 所指定的分区列的值需要使用英文引号包裹，而 EVERY 子句中的分区增量值不用英文引号包裹。

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
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
    "replication_num" = "3"
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

**建表后批量创建分区**

建表后，支持通过ALTER TABLE 语句批量创建分区。相关语法与建表时批量创建分区类似，通过指定 ADD PARTITIONS 关键字，以及 START、END 以及 EVERY 子句来批量创建分区。示例如下：

```SQL
ALTER TABLE site_access 
ADD PARTITIONS START ("2021-01-04") END ("2021-01-06") EVERY (INTERVAL 1 DAY);
```

### 管理分区

#### 增加分区

增加新的分区，用于存储新的数据。新增分区的默认分桶数量和原分区相同。您也可以根据新分区的数据规模调整分桶数量。

如下示例中，在 `site_access` 表添加新的分区，用于存储新月份的数据，并且调整分桶数量为 20：

```SQL
ALTER TABLE site_access
ADD PARTITION p4 VALUES LESS THAN ("2020-04-30")
DISTRIBUTED BY HASH(site_id);
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

## 设置分桶

### 随机分桶（自 v3.1）

对每个分区的数据，StarRocks 将数据随机地分布在所有分桶中，适用于数据量不大，对查询性能要求不高的场景。如果您不设置分桶方式，则默认由 StarRocks 使用随机分桶，并且自动设置分桶数量。

不过值得注意的是，如果查询海量数据且查询时经常使用一些列会作为条件列，随机分桶提供的查询性能可能不够理想。在该场景下建议您使用[哈希分桶](#哈希分桶)，当查询时经常使用这些列作为条件列时，只需要扫描和计算查询命中的少量分桶，则可以显著提高查询性能。

**使用限制**

- 仅支持明细模型表。
- 不支持指定 [Colocation Group](../using_starrocks/Colocate_join.md)。
- 不支持 [Spark Load](../loading/SparkLoad.md)。

如下建表示例中，没有使用 `DISTRIBUTED BY xxx` 语句，即表示默认由 StarRocks 使用随机分桶，并且由 StarRocks 自动设置分桶数量。

```SQL
CREATE TABLE site_access1(
    event_day DATE,
    site_id INT DEFAULT '10', 
    pv BIGINT DEFAULT '0' ,
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT ''
)
DUPLICATE KEY(event_day,site_id,pv);
```

当然，如果您比较熟悉 StarRocks 的分桶机制，使用随机分桶建表时，也可以手动设置分桶数量。

```SQL
CREATE TABLE site_access2(
    event_day DATE,
    site_id INT DEFAULT '10', 
    pv BIGINT DEFAULT '0' ,
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT ''
)
DUPLICATE KEY(event_day,site_id,pv)
DISTRIBUTED BY RANDOM BUCKETS 8; --手动设置分桶数量为 8
```

### 哈希分桶

对每个分区的数据，StarRocks 会根据分桶键和[分桶数量](#确定分桶数量)进行哈希分桶。在哈希分桶中，使用特定的列值作为输入，通过哈希函数计算出一个哈希值，然后将数据根据该哈希值分配到相应的桶中。

**优点**

- 提高查询性能。相同分桶键值的行会被分配到一个分桶中，在查询时能减少扫描数据量。
- 均匀分布数据。通过选取较高基数（唯一值的数量较多）的列作为分桶键，能更均匀的分布数据到每一个分桶中。

**如何选择分桶键**

假设存在列同时满足高基数和经常作为查询条件，则建议您选择其为分桶键，进行哈希分桶。 如果不存在这些同时满足两个条件的列，则需要根据查询进行判断。

- 如果查询比较复杂，则建议选择高基数的列为分桶键，保证数据在各个分桶中尽量均衡，提高集群资源利用率。
- 如果查询比较简单，则建议选择经常作为查询条件的列为分桶键，提高查询效率。

并且，如果数据倾斜情况严重，您还可以使用多个列作为数据的分桶键，但是建议不超过 3 个列。

**注意事项**

- **建表时，如果使用哈希分桶，则必须指定分桶键**。
- 分桶键指定后不支持修改。

如下示例中，`site_access` 表采用 `site_id` 作为分桶键，其原因在于 `site_id` 为高基数列。此外，针对 `site_access` 表的查询请求，基本上都以站点作为查询过滤条件，采用 `site_id` 作为分桶键，还可以在查询时裁剪掉大量无关分桶。

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day) (
    PARTITION p1 VALUES LESS THAN ("2020-01-31"),
    PARTITION p2 VALUES LESS THAN ("2020-02-29"),
    PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
DISTRIBUTED BY HASH(site_id);
```

如下查询中，假设每个分区有 10 个分桶，则其中 9 个分桶被裁减，因而系统只需要扫描 `site_access` 表中 1/10 的数据：

```SQL
select sum(pv)
from site_access
where site_id = 54321;
```

但是如果 `site_id` 分布十分不均匀，大量的访问数据是关于少数网站的（幂律分布，二八规则），那么采用上述分桶方式会造成数据分布出现严重的倾斜，进而导致系统局部的性能瓶颈。此时，您需要适当调整分桶的字段，以将数据打散，避免性能问题。例如，可以采用 `site_id`、`city_code` 组合作为分桶键，将数据划分得更加均匀。相关建表语句如下：

```SQL
CREATE TABLE site_access
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id,city_code);
```

在实际使用中，您可以依据自身的业务特点选择以上两种分桶方式。采用 `site_id` 的分桶方式对于短查询十分有利，能够减少节点之间的数据交换，提高集群整体性能；采用 `site_id`、`city_code` 的组合分桶方式对于长查询有利，能够利用分布式集群的整体并发性能。

> 说明：
>
> - 短查询是指扫描数据量不大、单机就能完成扫描的查询。
>
> - 长查询是指扫描数据量大、多机并行扫描能显著提升性能的查询。

### 确定分桶数量

在 StarRocks 中，分桶是实际物理文件组织的单元。

- 建表时如何设置分桶数量
  
  - 方式一：自动设置分桶数量（推荐）

    自 2.5.7 版本起， StarRocks 支持根据机器资源和数据量自动设置分区的分桶数量。

    建表示例：

    ```SQL
    CREATE TABLE site_access(
        site_id INT DEFAULT '10',
        city_code SMALLINT,
        user_name VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(site_id, city_code, user_name)
    DISTRIBUTED BY HASH(site_id,city_code); --无需手动设置分桶数量
    ```

    如果需要开启该功能，则您需要确保 FE 动态参数 `enable_auto_tablet_distribution` 为 `true`。
    建表后，您可以执行 [SHOW PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW%20PARTITIONS.md) 来查看 StarRock 为分区自动设置的分桶数量。

  - 方式二：手动设置分桶数量

    自 2.4 版本起，StarRocks 提供了自适应的 Tablet 并行扫描能力，即一个查询中涉及到的任意一个 Tablet 可能是由多个线程并行地分段扫描，减少了 Tablet 数量对查询能力的限制，从而可以简化对分桶数量的设置。简化后，确定分桶数量方式可以是：首先预估每个分区的数据量，然后按照每 10 GB 原始数据一个 Tablet 计算，从而确定分桶数量。

    如果需要开启并行扫描 Tablet，则您需要确保系统变量 `enable_tablet_internal_parallel` 全局生效 `SET GLOBAL enable_tablet_internal_parallel = true;`。

    ```SQL
    CREATE TABLE site_access (
        site_id INT DEFAULT '10',
        city_code SMALLINT,
        user_name VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0')
    AGGREGATE KEY(site_id, city_code, user_name)
    DISTRIBUTED BY HASH(site_id,city_code) BUCKETS 30; -- 假设导入一个分区的原始数据量为 300 GB，则按照每 10 GB 原始数据一个 Tablet，则分桶数量可以设置为 30。
    ```

- 新增分区时如何设置分桶数量

  - 方式一：自动设置分桶数量（推荐）

    自 2.5.7 版本起， StarRocks 支持根据机器资源和数据量自动设置分区的分桶数量。

    如果需要启用该功能，则您需要确保 FE 动态参数 `enable_auto_tablet_distribution` 保持默认值 `true`。如果需要关闭该功能，则您可以执行`ADMIN SET FRONTEND CONFIG ("enable_auto_tablet_distribution" = "false");`，并且新增分区的时候未指定分桶数量，则新增分区的分桶数量会继承建表时的分桶数量。
    新增分区后，您可以执行 [SHOW PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW%20PARTITIONS.md) 来查看 StarRocks 为新增分区自动设置的分桶数量。

  - 方式二：手动设置分桶数量

    您新增分区的时候，也可以手动指定分桶数量。新增分区的分桶数量的计算方式可以参考如上建表时手动设置分桶数量。

    ```SQL
    -- 手动创建分区
    ALTER TABLE <table_name> 
    ADD PARTITION <partition_name>
        [DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]];
        
    -- 手动设置动态分区的默认分桶数量
    ALTER TABLE <table_name> 
    SET ("dynamic_partition.buckets"="xxx");
    ```

> **注意**
>
> 不支持修改已创建分区的分桶数量。

## 最佳实践

对于 StarRocks 而言，分区和分桶的选择是非常关键的。在建表时选择合理的分区键和分桶键，可以有效提高集群整体性能。因此建议在选择分区键和分桶键时，根据业务情况进行调整。

- **数据倾斜**
  
  如果业务场景中单独采用倾斜度大的列做分桶，很大程度会导致访问数据倾斜，那么建议采用多列组合的方式进行数据分桶。

- **高并发**
  
  分区和分桶应该尽量覆盖查询语句所带的条件，这样可以有效减少扫描数据，提高并发。

- **高吞吐**
  
  尽量把数据打散，让集群以更高的并发扫描数据，完成相应计算。

- **元数据管理**

  Tablet 过多会增加 FE/BE 的元数据管理和调度的资源消耗。
