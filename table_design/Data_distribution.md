# 数据分布

## 名词解释

* **数据分布**：数据分布是将数据划分为子集, 按一定规则, 均衡地分布在不同节点上，以期最大限度地利用集群的并发性能。
* **短查询**：short-scan query，指扫描数据量不大，单机就能完成扫描的查询。
* **长查询**：long-scan query，指扫描数据量大，多机并行扫描能显著提升性能的查询。

## 数据分布概览

常见的四种数据分布方式有：(a) Round-Robin、(b) Range、(c) List和(d) Hash (DeWitt and Gray, 1992)。如下图所示:

![数据分布方式](../assets/3.3.2-1.png)

* **Round-Robin** : 以轮转的方式把数据逐个放置在相邻节点上。
* **Range** : 按区间进行数据分布，图中区间\[1-3\]，\[4-6\]分别对应不同Range。
* **List** : 直接基于离散的各个取值做数据分布，性别、省份等数据就满足这种离散的特性。每个离散值会映射到一个节点上，不同的多个取值可能也会映射到相同节点上。
* **Hash** : 按哈希函数把数据映射到不同节点上。  

为了更灵活地划分数据，现代分布式数据库除了单独采用上述四种数据分布方式之外，也会视情况采用组合数据分布。常见的组合方式有Hash-Hash、Range-Hash、Hash-List。

## StarRocks数据分布

### 1 数据分布方式

StarRocks使用先分区后分桶的方式, 可灵活地支持两种分布方式:

* Hash分布:  不采用分区方式, 整个table作为一个分区, 指定分桶的数量。
* Range-Hash的组合数据分布: 即指定分区数量, 指定每个分区的分桶数量。

~~~ SQL
-- 采用Hash分布的建表语句
CREATE TABLE site_access(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: 图3.2：采用Hash分布的建表语句

~~~ SQL
-- 采用Range-Hash组合分布的建表语句
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
PARTITION p1 VALUES LESS THAN ('2020-01-31'),
PARTITION p2 VALUES LESS THAN ('2020-02-29'),
PARTITION p3 VALUES LESS THAN ('2020-03-31')
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: 图3.3：采用Range-Hash组合分布的建表语句

StarRocks中Range分布，被称之为分区，用于分布的列也被称之为分区列，图3-3中的event\_day就是分区列。Hash分布，则被称之为分桶，用于分布的列也被称之为分桶列，图3-2、3-3中分桶列都是site\_id。

Range分区可动态添加和删减，图3-3中如果新来了隶属于新月份的数据，就可以添加新分区。Hash分桶一旦确定，分桶数也就随之固定，不能再进行调整。

采用Range-Hash组合分布方式的时候，新增Range分区，默认分桶数和原分区相同；用户可根据新分区的数据规模调整分桶数量。下图是一个在上表中添加分区，并使用新的分桶数的例子。

~~~ SQL
ALTER TABLE site_access
ADD PARTITION p4 VALUES LESS THAN ("2020-04-31")
DISTRIBUTED BY HASH(site_id) BUCKETS 20;
~~~

:-: 图3.4：在site_access表中添加新的分区，并使用新的分桶数

### 2 分区列如何选择

分区的主要作用是将整个分区作为管理单位, 选择存储策略, 比如副本数, 冷热策略和存储介质等等。大多数情况下，近期的数据被查询的可能性更大。将最近的数据放在一个分区之内，这样可以通过StarRocks的分区裁剪功能，最大限度地减少扫描数据量，从而提高查询性能。同时，StarRocks支持在一个集群内使用多种存储介质(SATA/SSD)。用户可以将最新数据所在的分区放在SSD上，利用SSD的随机读写性能来提高查询性能。而老的数据可以放在SATA盘上，以节省数据存储的成本。

所以，在实际应用中，用户一般选取时间列作为分区键，具体划分的粒度视数据量而定，单个分区原始数据量建议维持在100G以内。

### 3 分桶列如何选择

StarRocks采用Hash算法作为分桶算法，同一分区内, 分桶键的哈希值相同的数据形成(Tablet)子表, 子表多副本冗余存储, 子表副本在物理上由一个单独的本地存储引擎管理, 数据导入和查询最终都下沉到所涉及的子表副本上, 同时子表也是数据均衡和恢复的基本单位。

图3.2中，site\_access采用site\_id作为分桶键, 这里选用site\_id字段作为分桶键的原因在于，针对site\_access表的查询请求，基本上都以站点作为查询过滤条件。采用site\_id作为分桶键，可以在查询时裁剪掉大量无关分桶。如下图3.5中的查询，可以裁剪掉10个bucket中的9个，只需要扫描site\_access表的1/10的数据。

~~~ SQL
select
city_code, sum(pv)
from site_access
where site_id = 54321;
~~~

:-: 图3.5：site\_access表场景查询

但是存在这样一种情况，假设site\_id分布十分不均匀，大量的访问数据是关于少数网站的（幂律分布, 二八规则）。如果用户依然采用上述分桶方式，数据分布会出现严重的数据倾斜, 导致系统局部的性能瓶颈。这个时候，用户需要适当调整分桶的字段，以将数据打散，避免性能问题。如下图5中，可以采用site\_id、city\_code组合作为分桶键，将数据划分得更加均匀。

~~~ SQL
CREATE TABLE site_access
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id,city_code) BUCKETS 10;
~~~

:-: 图3.6：采用site\_id、city\_code作为分桶键

上面两种模式的选取是建表过程中经常需要面对的问题，采用site\_id的分桶方式对于短查询十分有利，能够减少节点之间的数据交换，提供集群整体性能；采用site\_id、city\_code组合做分桶键的模式对于长查询有利，能够利用分布式集群的整体并发性能，提高吞吐。用户在实际使用中，可以依据自身的业务特点进行相应模式的选择。

### 4 分桶数如何确定

在StarRocks系统中，分桶是实际物理文件组织的单元。数据在写入磁盘后，就会涉及磁盘文件的管理。一般而言，我们不建议分桶数据过大或过小，尽量适中会比较妥当。

分桶的数据的压缩方式使用的是Lz4。建议压缩后磁盘上每个分桶数据文件大小在 100MB-1GB 左右。这种模式在多数情况下足以满足业务需求。

建议用户根据集群规模的变化，建表时调整分桶的数量。集群规模变化，主要指节点数目的变化。假设现有20G原始数据，依照上述标准，可以建10个分桶（压缩前每个 tablet 大小2GB，压缩后可能在 200MB-500MB）。但是如果用户有20台机器，那么可以适当再缩小每个分桶的数据量，加大分桶数。比如分成40个分桶，大概每个 tablet 500MB（保持压缩后一般不小于 100MB）。

### 5 最佳实践

对于StarRocks而言，分区和分桶的选择是非常关键的。在建表时选择好的分区分桶列，可以有效提高集群整体性能。当然，在使用过程中，也需考虑业务情况，根据业务情况进行调整。

以下是针对特殊应用场景下，对分区和分桶选择的一些建议：

* **数据倾斜**：业务方如果确定数据有很大程度的倾斜，那么建议采用多列组合的方式进行数据分桶，而不是只单独采用倾斜度大的列做分桶。
* **高并发**：分区和分桶应该尽量覆盖查询语句所带的条件，这样可以有效减少扫描数据，提高并发。
* **高吞吐**：尽量把数据打散，让集群以更高的并发扫描数据，完成相应计算。

## 动态分区管理

在很多实际应用场景中，数据的时效性很重要，需要为新到达的数据创建新分区, 删除过期分区。 StarRocks的动态分区机制可以实现分区rollover:  对分区实现生命周期管理(TTL)，自动增删分区，减少用户的使用心智负担。

### 1 创建支持动态分区的表

下面以一个实际的例子来介绍动态分区功能。

~~~ SQL
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
~~~

:-: 图4.1：动态分区的表

图4.1中建表语句中通过指定PEROPERTIES来完成动态分区策略的配置。配置项可以描述如下：

* dynamic\_partition.enable : 是否开启动态分区特性，可指定为 TRUE 或 FALSE。如果不填写，默认为 TRUE。
* dynamic\_partition.time\_unit : 动态分区调度的粒度，可指定为 DAY/WEEK/MONTH。

  * 指定为 DAY 时，分区名后缀需为yyyyMMdd，例如20200325。图1 就是一个按天分区的例子，分区名的后缀满足yyyyMMdd。  
    PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),  
    PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),  
    PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),  
    PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
  * 指定为 WEEK 时，分区名后缀需为yyyy\_ww，例如2020\_13代表2020年第13周。
  * 指定为 MONTH 时，动态创建的分区名后缀格式为 yyyyMM，例如 202003。

* dynamic\_partition.start: 动态分区的开始时间。以当天为基准，超过该时间范围的分区将会被删除。如果不填写，则默认为Integer.MIN\_VALUE 即 -2147483648。
* dynamic\_partition.end: 动态分区的结束时间。 以当天为基准，会提前创建N个单位的分区范围。
* dynamic\_partition.prefix : 动态创建的分区名前缀。
* dynamic\_partition.buckets : 动态创建的分区所对应的分桶数量。

以图1中创建了一张表，并同步开启动态分区特性。图中分区的区间为当前时间的前后3天，总共6天。假设当前时间为2020-03-25为例，在每次调度时，会删除分区上界小于 2020-03-22 的分区，同时在调度时会创建今后3天的分区。调度完成之后，新的分区会是下列列表。

~~~ SQL
[types: [DATE]; keys: [2020-03-22]; ‥types: [DATE]; keys: [2020-03-23]; )
[types: [DATE]; keys: [2020-03-23]; ‥types: [DATE]; keys: [2020-03-24]; )
[types: [DATE]; keys: [2020-03-24]; ‥types: [DATE]; keys: [2020-03-25]; )
[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
~~~

:-: 图4.2：自动增删分区之后表的分区

调度的时机依赖于FE的配置，FE的常驻线程会根据dynamic\_partition\_enable和dynamic\_partition\_check\_interval\_seconds两个参数来控制。每次调度时，读取动态分区表的属性，以此判断是否增加/删除分区。

### 2 查看表当前的分区

动态分区表运行过程中，会不断地自动增减分区，可以通过下列命令查看当前的分区情况，

~~~ SQL
SHOW PARTITIONS FROM site_access;
~~~

:-: 图4.3 : site\_access当前分区

### 3 修改表的分区属性

动态分区的属性可以修改，例如需要起/停动态分区的功能，可以通过ALTER TABLE来完成。

~~~ SQL
ALTER TABLE site_access SET("dynamic_partition.enable"="false");
ALTER TABLE site_access SET("dynamic_partition.enable"="true");
~~~

注意：依照相同语句，也可以相应的修改其他属性。

### 4 注意事项

动态分区的方式相当于把建分区的判断逻辑交由StarRocks来完成，在配置的过程中一定要保证分区名称满足规范，否则会创建失败。具体规范可以描述如下：

* 指定为 DAY 时，分区名后缀需为yyyyMMdd，例如20200325。
* 指定为 WEEK 时，分区名后缀需为yyyy\_ww，例如 2020\_13, 代表2020年第13周。
* 指定为 MONTH 时，动态创建的分区名后缀格式为 yyyyMM，例如 202003。

## 批量创建和修改分区

> 该功能在1.16版本中添加

### 1 建表时批量创建日期分区

用户可以通过给出一个START值、一个END值以及一个定义分区增量值的EVERY子句批量产生分区。

其中START值将被包括在内而END值将排除在外。

例如：

~~~ SQL
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
    START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 day)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1" 
);
~~~

这样StarRocks便会自动创建如下等价的分区

~~~ TEXT
PARTITION p20210101 VALUES [('2021-01-01'), ('2021-01-02')),
PARTITION p20210102 VALUES [('2021-01-02'), ('2021-01-03')),
PARTITION p20210103 VALUES [('2021-01-03'), ('2021-01-04'))
~~~

当前分区键仅支持日期类型和整数类型，分区类型需要与EVERY里的表达式匹配。

当分区键为日期类型的时候需要指定INTERVAL关键字来表示日期间隔，目前日期仅支持day、week、month、year，分区的命名规则同动态分区一样。

### 2 建表时批量创建数字分区

当分区键为整数类型时直接使用数字进行分区，注意分区值需要使用引号引用，而EVERY则不用引号，如下：

~~~ SQL
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
~~~

上面的语句将产生如下分区：

~~~ SQL
PARTITION p1 VALUES [("1"), ("2")),
PARTITION p2 VALUES [("2"), ("3")),
PARTITION p3 VALUES [("3"), ("4")),
PARTITION p4 VALUES [("4"), ("5"))
~~~

### 3 建表时批量创建不同类型的日期分区

StarRocks也支持建表时同时定义不同类型的分区，只要求这些分区不相交，例如：

~~~ SQL
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

~~~

上面的语句将会产生如下分区：

~~~ TEXT
PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01')),
PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
PARTITION p20210501 VALUES [('2021-05-01'), ('2021-05-02')),
PARTITION p20210502 VALUES [('2021-05-02'), ('2021-05-03')),
PARTITION p20210503 VALUES [('2021-05-03'), ('2021-05-04'))
~~~

### 4 建表后批量创建分区

与建表时批量创建分区类似，StarRocks也支持通过ALTER语句批量创建分区。通过指定ADD PARTITIONS关键字，配合START和END以及EVERY的值来创建分区。

~~~ SQL
ALTER TABLE site_access ADD
PARTITIONS START ("2014-01-01") END ("2014-01-06") EVERY (interval 1 day);
~~~
