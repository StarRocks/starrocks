---
displayed_sidebar: docs
sidebar_position: 50
---

# Schema Tuning Recipes

本文档提供了通过有效的模式设计和基础表选择来优化 StarRocks 查询性能的实用技巧和最佳实践。通过了解不同表模型、键和分布策略如何影响查询执行，您可以显著提高速度和资源效率。使用这些指南在设计模式、选择表模型以及调整 StarRocks 环境以实现高性能分析时做出明智决策。

## 表模型选择

StarRocks 支持四种表模型：明细表、聚合表、更新表和主键表。所有这些表都是按键排序的。

- `AGGREGATE KEY`：当具有相同 AGGREGATE KEY 的记录导入到 StarRocks 时，新旧记录会被聚合。目前，聚合表支持以下聚合函数：SUM、MIN、MAX 和 REPLACE。聚合表支持提前聚合数据，方便业务报表和多维分析。
- `DUPLICATE KEY`：对于明细表，您只需指定排序键。具有相同 DUPLICATE KEY 的记录可以同时存在。适用于不涉及提前聚合数据的分析。
- `UNIQUE KEY`：当具有相同 UNIQUE KEY 的记录导入到 StarRocks 时，新记录会覆盖旧记录。更新表类似于具有 REPLACE 函数的聚合表。两者都适用于涉及持续更新的分析。
- `PRIMARY KEY`：主键表保证记录的唯一性，并允许您进行实时更新。

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


CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    browser      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid);

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid);

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
PRIMARY KEY(orderid)
DISTRIBUTED BY HASH(orderid);
```

## Colocate Table

为了加快查询速度，具有相同分布的表可以使用公共分桶列。在这种情况下，数据可以在本地进行连接，而无需在 `join` 操作期间在集群间传输。

```sql
CREATE TABLE colocate_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    browser      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid)
PROPERTIES(
    "colocate_with" = "group1"
);
```

有关 colocate join 和副本管理的更多信息，请参见 [Colocate join](../../using_starrocks/Colocate_join.md)

## 扁平表和星形模型

StarRocks 支持星形模型，这比扁平表在建模上更灵活。您可以在建模时创建视图以替换扁平表，然后从多个表中查询数据以加速查询。

扁平表有以下缺点：

- 维度更新成本高，因为扁平表通常包含大量维度。每次更新维度时，整个表都必须更新。随着更新频率的增加，情况会变得更糟。
- 维护成本高，因为扁平表需要额外的开发工作量、存储空间和数据回填操作。
- 数据摄取成本高，因为扁平表有很多字段，而聚合表可能包含更多的键字段。在数据导入期间，需要对更多字段进行排序，这会延长数据导入时间。

如果您对查询并发性或低延迟有较高要求，您仍然可以使用扁平表。

## 分区和分桶

StarRocks 支持两级分区：第一级是 RANGE 分区，第二级是 HASH 分桶。

- RANGE 分区：RANGE 分区用于将数据划分为不同的区间（可以理解为将原始表划分为多个子表）。大多数用户选择按时间设置分区，这具有以下优点：

  - 更容易区分冷热数据
  - 能够利用 StarRocks 分级存储（SSD + SATA）
  - 可以更快地按分区删除数据

- HASH 分桶：根据哈希值将数据划分为不同的桶。

  - 建议使用高区分度的列进行分桶，以避免数据倾斜。
  - 为了便于数据恢复，建议保持每个桶中压缩数据的大小在 100 MB 到 1 GB 之间。建议您在创建表或添加分区时配置适当的桶数量。
  - 不推荐随机分桶。创建表时必须显式指定 HASH 分桶列。

## 稀疏索引和 bloomfilter 索引

StarRocks 以有序方式存储数据，并以 1024 行的粒度构建稀疏索引。

StarRocks 在模式中选择固定长度的前缀（目前为 36 字节）作为稀疏索引。

创建表时，建议将常用的过滤字段放在模式声明的开头。区分度最高和查询频率最高的字段必须放在最前面。

VARCHAR 字段必须放在稀疏索引的末尾，因为索引从 VARCHAR 字段开始截断。如果 VARCHAR 字段出现在最前面，索引可能小于 36 字节。

以上述 `site_visit` 表为例。该表有四列：`siteid, city, username, pv`。排序键包含三列 `siteid, city, username`，分别占用 4、2 和 32 字节。因此，前缀索引（稀疏索引）可以是 `siteid + city + username` 的前 30 字节。

除了稀疏索引，StarRocks 还提供 bloomfilter 索引，这对于过滤高区分度的列非常有效。如果您希望将 VARCHAR 字段放在其他字段之前，可以创建 bloomfilter 索引。

## 倒排索引

StarRocks 采用 Bitmap 索引技术来支持倒排索引，可以应用于明细表的所有列以及聚合表和更新表的键列。Bitmap 索引适用于值范围较小的列，如性别、城市和省份。随着范围的扩大，bitmap 索引也会相应扩大。

## 物化视图（rollup）

Rollup 本质上是原始表（基表）的物化索引。创建 rollup 时，只能选择基表的一些列作为模式，并且模式中字段的顺序可以与基表不同。以下是使用 rollup 的一些用例：

- 基表中的数据聚合度不高，因为基表具有高区分度的字段。在这种情况下，您可以考虑选择一些列来创建 rollup。以上述 `site_visit` 表为例：

  ```sql
  site_visit(siteid, city, username, pv)
  ```

  `siteid` 可能导致数据聚合效果不佳。如果您需要频繁按城市计算 PV，可以创建仅包含 `city` 和 `pv` 的 rollup。

  ```sql
  ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
  ```

- 基表中的前缀索引无法命中，因为基表的构建方式无法覆盖所有查询模式。在这种情况下，您可以考虑创建 rollup 来调整列顺序。以上述 `session_data` 表为例：

  ```sql
  session_data(visitorid, sessionid, visittime, city, province, ip, browser, url)
  ```

  如果有需要按 `browser` 和 `province` 进行访问分析的情况，您可以创建一个单独的 rollup：

  ```sql
  ALTER TABLE session_data
  ADD ROLLUP rollup_browser(browser,province,ip,url)
  DUPLICATE KEY(browser,province);
  ```

## 模式变更

在 StarRocks 中有三种方式更改模式：排序模式变更、直接模式变更和链接模式变更。

- 排序模式变更：更改列的排序并重新排序数据。例如，在排序模式中删除列会导致数据重新排序。

  `ALTER TABLE site_visit DROP COLUMN city;`

- 直接模式变更：转换数据而不是重新排序数据，例如更改列类型或将列添加到稀疏索引。

  `ALTER TABLE site_visit MODIFY COLUMN username varchar(64);`

- 链接模式变更：无需转换数据即可完成更改，例如添加列。

  `ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';`

  建议在创建表时选择适当的模式以加速模式变更。