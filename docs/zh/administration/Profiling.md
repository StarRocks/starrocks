---
displayed_sidebar: "Chinese"
---

# 优化性能

本文介绍如何优化 StarRocks 性能。

## 通过建表优化性能

您可以在建表阶段通过以下方式优化 StarRocks 性能。

### 选择表类型

StarRocks 支持四种表类型：主键表 (PRIMARY KEY)，聚合表 (AGGREGATE KEY)，更新表 (UNIQUE KEY)，以及明细表 (DUPLICATE KEY)。四种类型的表中数据都是依据 KEY 进行排序。

* **聚合表**

    聚合表中，如果新写入的记录的 Key 列与表中的旧记录相同时，新旧记录被聚合，目前支持的聚合函数有 SUM，MIN，MAX，以及 REPLACE。聚合表可以提前聚合数据，适合报表和多维分析业务。

    ```sql
    CREATE TABLE site_visit
    (
        siteid      INT,
        city        SMALLINT,
        username    VARCHAR(32),
        pv BIGINT   SUM DEFAULT '0'
    )
    AGGREGATE KEY(siteid, city, username)
    DISTRIBUTED BY HASH(siteid);
    ```

* **更新表 (UNIQUE KEY)**

    更新表中，如果新写入的记录的 Key 列与表中的旧记录相同时，则新记录会覆盖旧记录。目前 UNIQUE KEY 的实现与 AGGREGATE KEY 的 REPLACE 聚合方法一样，二者本质上可以认为相同。更新表适用于有更新的分析业务。

    ```sql
    CREATE TABLE sales_order
    (
        orderid     BIGINT,
        status      TINYINT,
        username    VARCHAR(32),
        amount      BIGINT DEFAULT '0'
    )
    UNIQUE KEY(orderid)
    DISTRIBUTED BY HASH(orderid);
    ```

* **明细表 (DUPLICATE KEY)**

    明细表只用于排序，相同 DUPLICATE KEY 的记录会同时存在。明细表适用于数据无需提前聚合的分析业务。

    ```sql
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
    DISTRIBUTED BY HASH(sessionid, visitorid);
    ```

* **主键表 (PRIMARY KEY)**

    主键表保证同一个主键下仅存在一条记录。相对于更新表，主键表在查询时不需要执行聚合操作，并且支持谓词和索引下推，能够在支持实时和频繁更新等场景的同时，提供高效查询。

    ```sql
    CREATE TABLE orders (
        dt date NOT NULL,
        order_id bigint NOT NULL,
        user_id int NOT NULL,
        merchant_id int NOT NULL,
        good_id int NOT NULL,
        good_name string NOT NULL,
        price int NOT NULL,
        cnt int NOT NULL,
        revenue int NOT NULL,
        state tinyint NOT NULL
    )
    PRIMARY KEY (dt, order_id)
    DISTRIBUTED BY HASH(order_id);
    ```

### 使用 Colocate Table

StarRocks 支持将分布相同的相关表存储与共同的分桶列，从而相关表的 JOIN 操作可以直接在本地进行，进而加速查询。更多信息，参考 [Colocate Join](../using_starrocks/Colocate_join.md)。

```sql
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
DISTRIBUTED BY HASH(sessionid, visitorid)
PROPERTIES(
    "colocate_with" = "group1"
);
```

### 使用星型模型

StarRocks 支持选择更灵活的星型模型 (star schema) 来替代传统建模方式的大宽表。通过星型模型，您可以用一个视图来取代宽表进行建模，直接使用多表关联来查询。在 SSB 的标准测试集的对比中，StarRocks 的多表关联性能相较于单表查询并无明显下降。

相比星型模型，宽表的缺点包括：

* 维度更新成本更高。宽表中，维度信息更新会反应到整张表中，其更新的频率直接影响查询的效率。
* 维护成本更高。宽表的建设需要额外的开发工作、存储空间和数据 Backfill 成本。
* 导入成本更高。宽表的 Schema 字段数较多，聚合表中可能包含更多 Key 列，其导入过程中需要排序的列会增加，进而导致导入时间变长。

建议您优先使用星型模型，可以在保证灵活的基础上获得高效的指标分析效果。但如果您的业务对于高并发或者低延迟有较高的要求，您仍可以选择宽表模型进行加速。StarRocks 提供与 ClickHouse 相当的宽表查询性能。

### 使用分区和分桶

StarRocks支持两级分区存储，第一层为 RANGE 分区（Partition），第二层为 HASH 分桶（Bucket）。

RANGE 分区用于将数据划分成不同区间，逻辑上等同于将原始表划分成了多个子表。在生产环境中，多数用户会根据按时间进行分区。基于时间进行分区有以下好处：

* 可区分冷热数据。
* 可使用 StarRocks 分级存储（SSD + SATA）功能。
* 按分区删除数据时，更加迅速。

HASH 分桶指根据 Hash 值将数据划分成不同的 Bucket。

* 建议采用区分度大的列做分桶，避免出现数据倾斜。
* 为方便数据恢复，建议单个 Bucket 保持较小的 Size，应保证其中数据压缩后大小保持在 100MB 至 1GB 左右。建议您在建表或增加分区时合理考虑 Bucket 数目，其中不同分区可指定不同的 Bucket 数量。
* 不建议采用 Random 分桶方式。建表时，请指定明确的 Hash 分桶列。

### 使用稀疏索引和 Bloomfilter

StarRocks 支持对数据进行有序存储，在数据有序的基础上为其建立稀疏索引，索引粒度为 Block（1024 行）。

稀疏索引选取 Schema 中固定长度的前缀作为索引内容，目前 StarRocks 选取 36 个字节的前缀作为索引。

建表时，建议您将查询中常见的过滤字段放在 Schema 的前部。区分度越大，频次越高的查询字段应当被放置于更前部。VARCHAR 类型的字段只能作为稀疏索引的最后一个字段，因为索引会在 VARCHAR 字段处截断，VARCHAR 数据如果出现在前面，索引的长度可能不足 36 个字节。

例如，对于上述 `site_visit`表，其排序列包括 `siteid`，`city`，`username` 三列。其中，`siteid` 所占字节数为 4，`city` 所占字节数为 2，`username` 所占字节数为 32，所以该表的前缀索引的内容为 `siteid` + `city` + `username` 的前 30 个字节。

除稀疏索引之外，StarRocks 还提供 Bloomfilter 索引。Bloomfilter 索引对区分度比较大的列过滤效果明显。如果需要将 VARCHAR 字段前置，您可以建立 Bloomfilter 索引。

### 使用倒排索引

StarRocks 支持倒排索引，采用位图技术构建索引（Bitmap Index）。您可以将索引应用在明细表的所有列、聚合表和更新表的 Key 列上。位图索引适合取值空间较小的列，例如性别、城市、省份等信息列上。随着取值空间的增加，位图索引会同步膨胀。

### 使用物化视图

物化视图（Rollup）本质上可以理解为原始表（Base table）的一个物化索引。建立物化视图时，您可以只选取 Base table 中的部分列作为 schema，schema 中的字段顺序也可与 Base table 不同。下列情形可以考虑建立物化视图：

* Base table 中数据聚合度不高。这通常是因为 Base table 有区分度比较大的字段而导致。此时，您可以考虑选取部分列，建立物化视图。对于上述 `site_visit` 表，`siteid` 可能导致数据聚合度不高。如果有经常根据城市统计 `pv` 需求，可以建立一个只有 `city`，`pv` 的物化视图。

    ```sql
    ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
    ```

* Base table 中的前缀索引无法命中，这通常是因为 base table 的建表方式无法覆盖所有的查询模式。此时，您可以考虑调整列顺序，建立物化视图。对于上述 `session_data` 表，如果除了通过 `visitorid` 分析访问情况外，还有通过 `brower`，`province` 分析的情形，可以单独建立物化视图。

    ```sql
    ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);
    ```

## 优化导入性能

StarRocks 目前提供 Broker Load 和 Stream Load 两种导入方式，通过指定导入 label 标识一批次的导入。StarRocks 对单批次的导入会保证原子生效，即使单次导入多张表也同样保证其原子性。

* Stream Load：通过 HTTP 推流方式导入数据，用于微批导入。该模式下，1MB 数据导入延迟可维持在秒级别，适合高频导入。
* Broker Load：通过拉取的方式批量导入数据，适合大批量数据的导入。

## 优化 Schema Change 性能

StarRocks 目前支持三种 Schema Change 方式，即 Sorted Schema Change，Direct Schema Change，Linked Schema Change。

* Sorted Schema Change：改变列的排序方式，需对数据进行重新排序。例如删除排序列中的一列，字段重排序。

    ```sql
    ALTER TABLE site_visit DROP COLUMN city;
    ```

* Direct Schema Change：无需重新排序，但是需要对数据做一次转换。例如修改列的类型，在稀疏索引中加一列等。

    ```sql
    ALTER TABLE site_visit MODIFY COLUMN username varchar(64);
    ```

* Linked Schema Change：无需转换数据，直接完成。例如加列操作。

    ```sql
    ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';
    ```

建议您在建表时考虑好 Schema，以便加快后续 Schema Change 速度。
