# 性能优化

## 建表

### 数据模型选择

StarRocks数据模型目前分为三类: AGGREGATE KEY， UNIQUE KEY， DUPLICATE KEY。三种模型中数据都是按KEY进行排序。

* AGGREGATE KEY: AGGREGATE KEY相同时，新旧记录进行聚合，目前支持的聚合函数有SUM， MIN， MAX， REPLACE。 AGGREGATE KEY模型可以提前聚合数据，适合报表和多维分析业务。

~~~sql
CREATE TABLE site_visit
(
    siteid      INT,
    city        SMALLINT,
    username    VARCHAR(32),
    pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10;
~~~

UNIQUE KEY: UNIQUE KEY相同时，新记录覆盖旧记录。目前UNIQUE KEY实现上和AGGREGATE KEY的REPLACE聚合方法一样，二者本质上可以认为相同。适用于有更新的分析业务。

~~~sql
CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid) BUCKETS 10;
~~~

DUPLICATE KEY: 只指定排序列，相同DUPLICATE KEY的记录会同时存在。适用于数据无需提前聚合的分析业务。

~~~sql
CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10;
~~~

### 内存表

StarRocks支持把表数据全部缓存在内存中，用于加速查询，内存表适合数据行数不多维度表的存储。

~~~sql
CREATE TABLE memory_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10
PROPERTIES (
           "in_memory"="true"
);
~~~

### Colocate Table

为了加速查询，分布相同的相关表可以采用共同的分桶列。当分桶列相同时，相关表进行JOIN操作时，可以避免数据在集群中的传输，直接在本地进行JOIN。

~~~sql
CREATE TABLE colocate_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

更多关于colocate join的使用和副本管理机制参考[Colocate join](../using_starrocks/Colocation_join.md)

### 大宽表与star schema

业务方建表时, 为了和前端业务适配, 传统建模方式直接将schema定义成大宽表。对于StarRocks而言, 可以选择更灵活的星型模型来替代大宽表，比如用一个视图来取代宽表进行建模，直接使用多表关联来查询，比如在SSB的标准测试集的对比中，StarRocks的多表关联性能比单表查询下降并不太多。然后相比星型模型，宽表的缺点有：

1. 维度信息更新会反应到整张表中，而更新的频率直接影响查询的效率，宽表的更新成本。
2. 宽表的建设需要额外的开发工作、存储空间和数据backfill的成本。
3. schema中字段数比较多, 聚合模型中可能key列比较多, 导入过程中需要排序的列会增加，导致导入时间变长。

使用过程中，建议优先使用星型模型，可以在保证灵活的基础上获得高效的指标分析效果，但是对于有高并发或者低延迟要求的业务，还是可以选择宽表模型进行加速，StarRocks也可以提供与ClickHouse相当的宽表查询性能。

### 分区(partition)和分桶(bucket)

StarRocks支持两级分区存储, 第一层为RANGE分区(partition), 第二层为HASH分桶(bucket)。

* RANGE分区(partition) : RANGE分区用于将数据划分成不同区间, 逻辑上可以理解为将原始表划分成了多个子表。 业务上，多数用户会选择采用按时间进行partition, 让时间进行partition有以下好处：

* 可区分冷热数据
* 可用上StarRocks分级存储(SSD + SATA)的功能
* 按分区删除数据时，更加迅速

* HASH分桶(bucket) : 根据hash值将数据划分成不同的bucket。

* 建议采用区分度大的列做分桶, 避免出现数据倾斜
* 为方便数据恢复, 建议单个bucket的size不要太大, 单个bucket中数据压缩后大小保持在100M-1GB左右,所以建表或增加partition时请合理考虑buckets数目, 其中不同partition可指定不同的buckets数。
* random分桶的方式不建议采用，建表时烦请指定明确的hash分桶列。

### 稀疏索引和bloomfilter

StarRocks对数据进行有序存储, 在数据有序的基础上为其建立稀疏索引,索引粒度为block(1024行)。

* 稀疏索引选取schema中固定长度的前缀作为索引内容, 目前StarRocks选取36个字节的前缀作为索引。

* 建表时建议将查询中常见的过滤字段放在schema的前面, 区分度越大，频次越高的查询字段越往前放。
* 这其中有一个特殊的地方,就是varchar类型的字段,varchar类型字段只能作为稀疏索引的最后一个字段，索引会在varchar处截断, 因此varchar如果出现在前面，可能索引的长度不足36个字节。

* 对于上述site_visit表

    `site_visit(siteid, city, username, pv)`

* 排序列有siteid, city, username三列, siteid所占字节数为4, city所占字节数为2，username占据32个字节, 所以前缀索引的内容为siteid + city + username的前30个字节
* 除稀疏索引之外, StarRocks还提供bloomfilter索引, bloomfilter索引对区分度比较大的列过滤效果明显。 如果考虑到varchar不能放在稀疏索引中, 可以建立bloomfilter索引。

### 倒排索引

StarRocks支持倒排索引，采用位图技术构建索引(Bitmap Index)。索引能够应用在 Duplicate 数据模型的所有列和 Aggregate, Uniqkey 模型的Key列上，位图索引适合取值空间不大的列，例如性别、城市、省份等信息列上。随着取值空间的增加，位图索引会同步膨胀。

### 物化视图(rollup)

Rollup本质上可以理解为原始表(base table)的一个物化索引。建立rollup时可只选取base table中的部分列作为schema，schema中的字段顺序也可与base table不同。 下列情形可以考虑建立rollup:

* base table中数据聚合度不高，这一般是因base table有区分度比较大的字段而导致。此时可以考虑选取部分列，建立rollup。 对于上述site_visit表

    `site_visit(siteid, city, username, pv)`

* siteid可能导致数据聚合度不高，如果业务方经常根据城市统计pv需求，可以建立一个只有city, pv的rollup。

    `ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);`

* base table中的前缀索引无法命中，这一般是base table的建表方式无法覆盖所有的查询模式。此时可以考虑调整列顺序，建立rollup。对于上述session_data表

    `session_data(visitorid, sessionid, visittime, city, province, ip, brower, url)`

* 如果除了通过 visitorid 分析访问情况外，还有通过brower, province分析的情形，可以单独建立rollup。

    `ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);`

## 导入

StarRocks目前提供broker load和stream load两种导入方式， 通过指定导入label标识一批次的导入。StarRocks对单批次的导入会保证原子生效， 即使单次导入多张表也同样保证其原子性。

* stream load : 通过http推的方式进行导入，微批导入。1MB数据导入延迟维持在秒级别，适合高频导入。
* broker load : 通过拉的方式导入， 适合天级别的批量数据的导入。

## schema change

StarRocks中目前进行schema change的方式有三种，sorted schema change，direct schema change， linked schema change。

* sorted schema change: 改变了列的排序方式，需对数据进行重新排序。例如删除排序列中的一列， 字段重排序。

    `ALTER TABLE site_visit DROP COLUMN city;`

* direct schema change: 无需重新排序，但是需要对数据做一次转换。例如修改列的类型，在稀疏索引中加一列等。

    `ALTER TABLE site_visit MODIFY COLUMN username varchar(64);`

* linked schema change: 无需转换数据，直接完成。例如加列操作。

    `ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';`

    建表时建议考虑好schema，这样在进行schema change时可以加快速度。
