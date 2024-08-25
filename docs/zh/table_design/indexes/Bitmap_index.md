---
displayed_sidebar: docs
keywords: ['suoyin']
---

# Bitmap 索引

本文介绍了如何创建和管理 Bitmap（位图）索引，以及 Bitmap 索引的使用案例。

## 功能简介

Bitmap 索引是一种使用 bitmap 的特殊数据库索引。bitmap 即为一个 bit 数组，一个 bit 的取值有两种：0 或 1。每一个 bit 对应数据表中的一行，并根据该行的取值情况来决定 bit 的取值是 0 还是 1。

如果查询的过滤条件命中前缀索引，可以显著提高查询效率，快速返回结果。但是一个表只能有一个前缀索引，如果查询的过滤条件没有包含前缀索引的前缀，则为了提高这类查询的效率可以为该列创建 Bitmap 索引。

### 如何合理设计 Bitmap 索引，以便加速查询

选择 Bitmap 索引的首要考虑因素是**列的基数和 Bitmap 索引对查询的过滤效果。**与普遍观念相反，Bitmap 索引比较适用于**较高基数列的查询和多个低基数列的组合查询，此时 Bitmap 索引对查询的过滤效果比较好，至少可以过滤掉 999/1000 的数据**，能够过滤较多的 Page 数据。

如果是单个低基数列的查询，那么 Bitmap 索引过滤效果不佳，待读取的数据行较多并且散落在较多 Page 中。

:::info

评估 Bitmap 索引对查询的过滤效果时，需要看加载数据的开销。并且 StarRocks 中底层数据以 Page（默认为 64K）为单位组织和加载的，加载数据的开销主要有几个部分：从磁盘加载 Page 的 IO 时间，Page 解压缩和解码时间。

:::

然而如果基数过于高，也会带来其他问题，比如**占用较多的磁盘空间**，并且因为需要导入时需要构建 Bitmap 索引，导入频繁时则**导入性能会受影响**。

并且还需要考虑**查询时加载 Bitmap 索引的开销**。因为查询时候只会按需加载 Bitmap 索引，即 `查询条件涉及的列值数量/基数 x Bitmap 索引`。这一值越大，则查询时加载的 Bitmap 索引开销也越大。

因此为了确定 Bitmap 索引适合列的基数和查询，建议您参考本文的 [Bitmap 索引性能测试](#Bitmap 索引性能测试)，根据实际业务数据和查询进行性能测试：**在不同基数的列上使用 Bitmap 索引，分析和权衡 Bitmap 索引对于查询过滤效果（至少可以过滤掉 999/1000 的数据），以及带来的磁盘空间占用，导入性能的影响，和查询时加载 Bitmap 索引的开销等额外影响。**

不过，您创建的 Bitmap 索引无法加速查询，例如无法过滤较多 Page 数据，或者查询时加载 Bitmap开销较大，由于 [StarRocks 本身对于查询有 Bitmap 索引的自适应选择机制](#自适应选择是否使用-bitmap-索引)，如果 Bitmap 索引不合适，则查询时不会使用 Bitmap 索引，所以查询性能也不会受到明显影响。

### 自适应选择是否使用 Bitmap 索引

StarRocks 会针对列基数和查询自适应选择是否使用 Bitmap 索引。如果用 Bitmap 索引起不到过滤较多 Page 数据的效果，或者查询时加载 Bitmap 索引开销较大，则 StarRocks 默认不会使用该  Bitmap 索引，避免使用后导致查询性能变差。

StarRocks 判断是否使用 Bitmap 索引的逻辑：因为一般情况下查询条件涉及的列值数量/列的基数，这一值越小，说明 Bitmap 索引过滤效果好。所以 StarRocks 采用 `bitmap_max_filter_ratio/1000`作为判断阈值，当过滤条件涉及比较值的数量 /列的基数 < `bitmap_max_filter_ratio/1000` 时才会用 Bitmap 索引。`bitmap_max_filter_ratio` 默认为 `1`。

首先以单列查询为例，比如表 `employees` 中 `gender` 列的值有 `male` 和 `gender` 列为 `female` 。执行查询`SELECT * FROM employees WHERE gender = 'male';`时，因为性别列的基数为 2，比较低（只有 `male` 和 `female` 2 个不同值），查询条件只涉及其中一个值，此时查询条件涉及的列值数量/列的基数等于 1/2，该值大于 1/1000，所以该查询不会使用 Bitmap 索引。

再以多列组合查询 `SELECT * FROM employees WHERE gender = 'male' AND city IN ('北京', '上海');` 为例，假设 City 列的基数为 10000，比较高，查询条件涉及其中 2 个值，此时查询条件涉及的列值数量/列的基数的计算方式是两列相乘，结果为 `(1*2)/(2*10000)`，该值小于 1/1000，所以该查询会使用 Bitmap 索引。

:::info

`bitmap_max_filter_ratio` 的取值范围为 1~1000。如果取值为 `1000`，则表示只要查询的列有 Bitmap 索引，就会强制使用 Bitmap 索引。

:::

### 优势

- 能够快速定位查询的列值所在的数据行号，适用于点查或是小范围查询。
- 适用于交并集运算（OR 和 AND 运算），可以优化多维查询。  

## 注意事项

### 支持优化的查询

Bitmap 索引适用于优化等值 `=` 查询、`[NOT] IN` 范围查询、`>`，`>=`，`<`，`<=` 查询、 `IS NULL` 查询。不适用于优化 `!=`，`[NOT] LIKE` 查询。

### 支持的列和列的数据类型

主键表和明细表中所有列都可以创建 Bitmap 索引；聚合表和更新表中，只有 Key 列支持创建 Bitmap 索引。 支持为如下类型的列创建 Bitmap 索引：

- 日期类型：DATE、DATETIME。
- 数值类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DECIMAL 和 BOOLEAN。
- 字符串类型：CHAR、STRING 和 VARCHAR。
- 其他类型：HLL。

## 基本操作

### 创建索引

- 建表时创建 Bitmap 索引。

    ```SQL
    CREATE TABLE `lineorder_partial` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
       INDEX lo_orderdate_index (lo_orderdate) USING BITMAP
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

   本示例中，创建了基于 `lo_orderdate` 列创建了名称为 `lo_orderdate_index` 的 Bitmap 索引。Bitmap 索引的命名要求参见[系统限制](../../sql-reference/System_limit.md)。在同一张表中不能创建名称相同的 Bitmap 索引。

   并且，支持为多个列创建 Bitmap 索引，并且多个 Bitmap 索引定义之间用英文逗号（,）隔开。

  :::note

  建表的其他参数说明，参见 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

  :::

- 建表后使用 CREATE INDEX 创建 Bitmap 索引。详细参数说明和示例，参见 [CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md)。

    ```SQL
    CREATE INDEX lo_quantity_index (lo_quantity) USING BITMAP;
    ```

### 创建进度

创建 Bitmap 索引为**异步**过程，执行索引创建语句后可通过 [SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) 命令查看索引创建进度，当返回值中 `State` 字段显示为 `FINISHED` 时，即为创建成功。

```SQL
SHOW ALTER TABLE COLUMN [FROM db_name];
```

:::info

当前每张表只允许同时进行一个 Schema Change 任务，在一个 Bitmap 索引未异步创建完成前，无法进行新 Bitmap 索引的创建。

:::

### 查看索引

查看指定表的所有 Bitmap 索引。详细参数和返回结果说明，参见 [SHOW INDEX](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_INDEX.md)。

```SQL
SHOW { INDEX[ES] | KEY[S] } FROM [db_name.]table_name [FROM db_name];
```

:::note

创建 Bitmap 索引为异步过程，使用如上语句只能查看到已经创建完成的索引。

:::

### 删除索引

删除指定表的 Bitmap 索引。详细参数说明和示例，参见 [DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md)。

```SQL
DROP INDEX index_name ON [db_name.]table_name;
```

### 查看查询是否命中了 Bitmap 索引

查看该查询的 Profile 中的 `BitmapIndexFilterRows` 字段。关于如何查看 Profile，参见[分析查询](../../administration/Query_planning.md#查看分析-profile)。

## Bitmap 索引性能测试

### 测试目的

在如下不同基数的列上使用 Bitmap 索引，分析 Bitmap 索引对查询过滤效果以及其他影响，比如磁盘占用等。

- [查询单个低基数列](#查询一基于单个低基数列的查询)
- [基于多个低基数列进行组合查询](#查询二基于多个低基数列的组合查询)
- [查询单个高基数列](#查询三基于单个高基数列的查询)

并且本小节还会对比强制使用 Bitmap 索引和 StarRocks 针对查询自适应选择是否 Bitmap 索引，来验证 [StarRocks 根据查询自适应选择是否使用 Bitmap 索引](#自适应选择是否使用-bitmap-索引)的效果。

### 建表和创建 Bitmap 索引

:::warning

为了避免缓存 Page 影响查询性能，需要确保 BE 配置项 `disable_storage_page_cache` 为 `true`。

:::

本小节以 SSB 20G 的 lineorder 表（具有 1.4 亿行数据）为例说明。

- 原始表 (没有创建 Bitmap 索引)---作为参照对象。

    ```SQL
    CREATE TABLE `lineorder_without_index` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_linenumber` int(11) NOT NULL COMMENT "",
      `lo_custkey` int(11) NOT NULL COMMENT "",
      `lo_partkey` int(11) NOT NULL COMMENT "",
      `lo_suppkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_shippriority` int(11) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_extendedprice` int(11) NOT NULL COMMENT "",
      `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
      `lo_discount` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
      `lo_supplycost` int(11) NOT NULL COMMENT "",
      `lo_tax` int(11) NOT NULL COMMENT "",
      `lo_commitdate` int(11) NOT NULL COMMENT "",
      `lo_shipmode` varchar(11) NOT NULL COMMENT ""
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

- 具有 Bitmap 索引的表：基于 `lo_shipmode`，`lo_quantity`， `lo_discount`，`lo_orderdate`， `lo_tax`，`lo_partkey` 创建 Bitmap 索引。

    ```SQL
    CREATE TABLE `lineorder_with_index` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_linenumber` int(11) NOT NULL COMMENT "",
      `lo_custkey` int(11) NOT NULL COMMENT "",
      `lo_partkey` int(11) NOT NULL COMMENT "",
      `lo_suppkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_shippriority` int(11) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_extendedprice` int(11) NOT NULL COMMENT "",
      `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
      `lo_discount` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
      `lo_supplycost` int(11) NOT NULL COMMENT "",
      `lo_tax` int(11) NOT NULL COMMENT "",
      `lo_commitdate` int(11) NOT NULL COMMENT "",
      `lo_shipmode` varchar(11) NOT NULL COMMENT "",
      INDEX i_shipmode (`lo_shipmode`) USING BITMAP,
      INDEX i_quantity (`lo_quantity`) USING BITMAP,
      INDEX i_discount (`lo_discount`) USING BITMAP,
      INDEX i_orderdate (`lo_orderdate`) USING BITMAP,
      INDEX i_tax (`lo_tax`) USING BITMAP,
      INDEX i_partkey (`lo_partkey`) USING BITMAP
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

### Bitmap 索引占用磁盘空间情况

- lo_shipmode 字符串类型，基数为7, Bitmap 索引占用磁盘空间 130M
- lo_quantity: 整数类型，基数为 50, Bitmap 索引占用磁盘空间 291M
- lo_discount: 整数类型，基数为 11, Bitmap 索引占用磁盘空间 198M
- lo_orderdate: 整数类型，基数为 2406, Bitmap 索引占用磁盘空间 191M
- lo_tax: 整数类型，基数为 9, Bitmap 索引占用磁盘空间为 160M
- lo_partkey: 整数类型，基数为 60万，Bitmap索引占用磁盘空间为 601M

### 查询一：基于单个低基数列的查询

#### 查询没有创建 Bitmap 索引的表

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_without_index WHERE lo_shipmode="MAIL";
```

**查询性能分析**：因为查询的表没有 Bitmap 索引，所以查询时会将包含 `lo_shipmode` 列数据的 Page 全部读出来，再进行谓词过滤。

总共耗时约 0.91 ms，**其中加载数据花了 0.47 ms**，低基数优化字典解码花了 0.31 ms，谓词过滤花了 0.23 ms。

```Bash
PullRowNum: 20.566M (20566493) // 返回结果集的行数。
CompressedBytesRead: 55.283 MB // 读取的总数据量。
RawRowsRead: 143.999M (143999468) // 读取的数据行。因为没有 Bitmap 索引，这一列的数据全部读出来。
ReadPagesNum: 8.795K (8795) // 读取的 Page 数量。因为没有 Bitmap 索引，这一列数据所在的 Page 全部读出来。
IOTaskExecTime: 914ms // Scan 数据的总时间。
    BlockFetch: 469ms // 加载数据时间。
    DictDecode: 311.612ms // 低基数优化字典解码的时间。
    PredFilter: 23.182ms // 谓词过滤的时间。
    PredFilterRows: 123.433M (123432975) // 过滤掉的行数。
```

#### 查询具有 Bitmap 索引的表

##### 强制使用 Bitmap 索引

:::info

为了强制使用 Bitmap 索引，因此根据 Starrocks的配置，必须在各个 BE 节点的  **be.conf** 文件中设置 `bitmap_max_filter_ratio=1000`，然后重启 BE 节点。

:::

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL";
```

**查询性能分析**：由于查询的列是低基数列，因此 Bitmap 索引对查询过滤效果不佳，即使 Bitmap 索引能够快速定位到实际数据的行号，但是待读取的数据行较多并且散落在各个 Page 中，因此实际上无法过滤掉需要读取的 Page。反而因为强制使用 Bitmap 索引，引入了加载 Bitmap 索引和使用 Bitmap 索引过滤数据的开销，总时间更多。

总共耗时 2.7s，**其中加载数据和 Bitmap 索引花了 0.93s**，低基数优化字典解码花了 0.33s，使用 Bitmap 索引过滤数据花了 0.42s，ZoneMap 索引过滤数据花了 0.17s。

```Bash
PullRowNum: 20.566M (20566493) // 返回结果集的行数。
CompressedBytesRead: 72.472 MB // 读取的总数据量。该列 Bitmap 索引总大小是 130M，具有 7 个唯一值，单个值的 Bitmap 索引大小为 18M，再加上 Page 数据（55M）= 73M。
RawRowsRead: 20.566M (20566493) // 读取的数据行。实际只读了 2000 万行。
ReadPagesNum: 8.802K (8802) // 读取的 Page 数量。实际上还是读取所有 Page。因为使用 Bitmap 索引过滤出的这 2000 万行数据是随机分布在各个 Page 的，而 Page 是最小数据读取单元，所以 Bitmap 索引没有过滤掉 Page。
IOTaskExecTime: 2s77ms // Scan 数据总时间，相对于没创建 Bitmap 索引耗时更长。
    BlockFetch: 931.144ms // 加载数据和 Bitmap 索引时间。相比于上一个查询，该查询为了加载 Bitmap 索引（18M），多花了 400ms
    DictDecode: 329.696ms // 因为输出的行数是一样的，所以低基数优化字典解码的时间所花时间差不多
    BitmapIndexFilter: 419.308ms // Bitmap 索引过滤数据的时间。
    BitmapIndexFilterRows: 123.433M (123432975) // Bitmap 索引过滤掉的数据行数。
    ZoneMapIndexFiter: 171.580ms // ZoneMap 索引过滤数据花了 0.17s。
```

##### 由 StarRocks 默认配置决定是否使用 Bitmap 索引

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL";
```

**查询性能分析**：因为根据 StarRocks 默认的配置，过滤条件中涉及的列值数量/列的基数 < `bitmap_max_filter_ratio/1000`（默认为 1/1000）才会走 Bitmap 索引，然而实际上该值大于 1/1000，因此查询没有使用 Bitmap 索引，查询效果等同于查询没有创建 Bitmap 索引的表。

```Bash
PullRowNum: 20.566M (20566493) // 返回结果集的行数。
CompressedBytesRead: 55.283 MB // 读取的总数据量。
RawRowsRead: 143.999M (143999468) // 读取的数据行。因为没有使用 Bitmap 索引，这一列的数据全部读出来。
ReadPagesNum: 8.800K (8800) // 读取的 Page 数量。因为没有使用 Bitmap 索引，这一列数据所在的 Page 全部读出来。
IOTaskExecTime: 914.279ms // Scan 数据的总时间。
    BlockFetch: 475.890ms // 加载数据时间。
    DictDecode: 312.019ms // 低基数优化字典解码的时间。
    PredFilter: 17.311ms // 谓词过滤的时间。
    PredFilterRows: 123.433M (123432975) // 过滤掉的行数。
```

### 查询二：基于多个低基数列的组合查询

#### 查询没有创建 Bitmap 索引的表

**查询语句**：

```SQL
SELECT count(1) 
FROM lineorder_without_index 
WHERE lo_shipmode = "MAIL" 
  AND lo_quantity = 10 
  AND lo_discount = 9 
  AND lo_tax = 8;
```

**查询性能分析**：因为查询的表没有 Bitmap 索引，所以查询时会将包含 `lo_shipmode` 、`lo_quantity`、`lo_discount` 和 `lo_tax` 这四列数据的 Page 全部读出来，再进行谓词过滤。

总共耗时 1.76s，**其中加载数据（4 个列的数据）花了 1.6s**，谓词过滤花了 0.1s。

```Bash
PullRowNum: 4.092K (4092) // 返回结果集的行数。
CompressedBytesRead: 305.346 MB // 读取的总数据量。读取了 4 列，总共 305M。
RawRowsRead: 143.999M (143999468) // 读取的数据行。因为没有 Bitmap 索引，这 4 列的数据全部读出来。
ReadPagesNum: 35.168K (35168) // 读取的 Page 数量。因为没有 Bitmap 索引，这 4 列的数据所在的 Page 全部读出来。
IOTaskExecTime: 1s761ms // Scan 数据的总时间
    BlockFetch: 1s639ms //  加载数据时间
    PredFilter: 96.533ms // 使用 4 个过滤条件进行谓词过滤的时间
    PredFilterRows: 143.995M (143995376) // 谓词过滤掉的行数
```

#### 查询具有 Bitmap 索引的表

##### 强制使用 Bitmap 索引

:::info

为了强制使用 Bitmap 索引，因此根据 Starrocks的配置，必须在各个 BE 节点的  **be.conf** 文件中设置 `bitmap_max_filter_ratio=1000`，然后重启 BE 节点。

:::

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL" AND lo_quantity=10 AND lo_discount=9 AND lo_tax=8;
```

**查询性能分析**：由于是基于多个低基数列的组合查询，Bitmap 索引效果较好，能够过滤掉一部分 Page，读取数据的时间明显减少。

总共耗时 0.68s，**其中加载数据和 Bitmap 索引花了 0.54s**，Bitmap 索引过滤数据花了 0.14s。

```Bash
PullRowNum: 4.092K (4092) // 返回结果集的行数。
CompressedBytesRead: 156.340 MB // 读取的总数据量。Bitmap 索引的过滤效果比较好，过滤掉了 2/3 数据。这 156M中，其中索引占 60M, 数据占 90M。
ReadPagesNum: 11.325K (11325) // 读取的 Page 数量。Bitmap 索引的过滤效果比较好，过滤掉了 2/3 的 Page。
IOTaskExecTime: 683.471ms // Scan 数据的总时间，相对于没创建 Bitmap 索引耗时显著减少。
    BlockFetch: 537.421ms // 加载数据和 Bitmap 索引所需时间。相比于上个查询，虽然增加了加载 Bitmap 索引的时间，但是大大减少了加载数据的时间。
    BitmapIndexFilter: 139.024ms // Bitmap 索引过滤数据的时间。
    BitmapIndexFilterRows: 143.995M (143995376)  // Bitmap 索引过滤掉的数据行数。
```

##### 由 StarRocks 的默认配置决定是否使用 Bitmap 索引

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL" AND lo_quantity=10 AND lo_discount=9 AND lo_tax=8;
```

**查询性能分析**：因为根据 StarRocks 默认的配置，过滤条件中涉及的列值数量/列的基数 < `bitmap_max_filter_ratio/1000`（默认为 1/1000）才会走 Bitmap 索引。实际上该值确实小于 1/1000，因此查询使用 Bitmap 索引，查询效果等同于强制使用 Bitmap 索引。

总共耗时 0.67s，**其中加载数据和 Bitmap 索引花了 0.54s**，Bitmap 索引过滤数据花了 0.13s。

```Bash
PullRowNum: 4.092K (4092) // 返回结果集的行数。
CompressedBytesRead: 154.430 MB // 读取的总数据量。
ReadPagesNum: 11.209K (11209) // 读取的 Page 数量。
IOTaskExecTime: 672.029ms // Scan 数据的总时间，相对于没创建 Bitmap 索引耗时显著减少。
    BlockFetch: 535.823ms // 加载数据和 Bitmap 索引的时间。相比于上个查询，虽然增加了加载 Bitmap 索引的时间，但是大大减少了加载数据的时间。
    BitmapIndexFilter: 128.403ms // Bitmap 索引过滤数据的时间。
    BitmapIndexFilterRows: 143.995M (143995376) // Bitmap 索引过滤掉的数据行数。
```

### 查询三：基于单个高基数列的查询

#### 查询没有创建 Bitmap 索引的表

**查询语句**：

```SQL
select count(1) from lineorder_without_index where lo_partkey=10000;
```

**查询性能分析**：因为查询的表没有 Bitmap 索引，所以查询时会将包含 `lo_partkey` 列数据的 page 全部读出来，再进行谓词过滤。

总共耗时约 0.43 ms，**其中加载数据花了 0.39 ms**，谓词过滤花了 0.02 ms。

```Bash
PullRowNum: 255 // 返回结果集的行数。
CompressedBytesRead: 344.532 MB // 读取的总数据量。
RawRowsRead: 143.999M (143999468) // 读取的数据行。因为没有 Bitmap 索引，这一列的数据全部读出来。
ReadPagesNum: 8.791K (8791) // 读取的 Page 数量。因为没有 Bitmap 索引，这一列数据所在的 Page 全部读出来。
IOTaskExecTime: 428.258ms // Scan 数据的总时间。
    BlockFetch: 392.434ms // 加载数据时间。
    PredFilter: 20.807ms // 谓词过滤的时间。
    PredFilterRows: 143.999M (143999213) // 过滤掉的行数。
```

#### 查询具有 Bitmap 索引的表

##### 强制使用 Bitmap 索引

:::info

为了强制使用 Bitmap 索引，因此根据 Starrocks的配置，必须在各个 BE 节点的  **be.conf** 文件中设置 `bitmap_max_filter_ratio=1000`，然后重启 BE 节点。

:::

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_partkey=10000;
```

**查询性能分析**：由于查询的列是基数较高，因此 Bitmap 索引效果比较好，能够过滤掉一部分 Page，读取数据的时间明显减少。

总共耗时 0.015s，**其中加载数据和 Bitmap 索引花了 0.009s**，Bitmap 索引过滤数据花了 0.003s。

```Bash
PullRowNum: 255 // 返回结果集的行数。
CompressedBytesRead: 13.600 MB // 读取的总数据量。Bitmap 索引的过滤效果比较好，过滤掉了较多的数据。
RawRowsRead: 255 // 读取的数据行。
ReadPagesNum: 225 // 读取的 Page 数量。Bitmap 索引的过滤效果比较好，过滤掉较多的 Page。
IOTaskExecTime: 15.354ms  // Scan 数据的总时间，相对于没创建 Bitmap 索引耗时显著减少。
    BlockFetch: 9.530ms // 加载数据和 Bitmap 索引所需时间。相比于上个查询，虽然增加了加载 Bitmap 索引的时间，但是大大减少了加载数据的时间。
    BitmapIndexFilter: 3.450ms // Bitmap 索引过滤数据的时间。
    BitmapIndexFilterRows: 143.999M (143999213) // Bitmap 索引过滤掉的数据行数。
```

##### 由 StarRocks 的默认配置决定是否使用 Bitmap 索引

**查询语句**：

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_partkey=10000;
```

**查询性能分析**：因为根据 StarRocks 默认的配置，过滤条件中涉及的列值数量/列的基数 < `bitmap_max_filter_ratio/1000`（默认为 1/1000）才会走 Bitmap 索引。实际上该值确实小于 1/1000，因此查询使用 Bitmap 索引，查询效果等同于强制使用 Bitmap 索引。

总共耗时 0.014s，**其中加载数据和 Bitmap 索引花了 0.008s**，Bitmap 索引过滤数据花了 0.003s。

```Bash
PullRowNum: 255 // 返回结果集的行数。
CompressedBytesRead: 13.600 MB // 读取的总数据量。Bitmap 索引的过滤效果比较好，过滤掉了较多的数据。
RawRowsRead: 255 // 读取的数据行。
ReadPagesNum: 225 // 读取的 Page 数量。Bitmap 索引的过滤效果比较好，过滤掉较多的 Page。
IOTaskExecTime: 14.387ms // Scan 数据的总时间，相对于没创建 Bitmap 索引耗时显著减少。
    BitmapIndexFilter: 2.979ms // Bitmap 索引过滤数据的时间。
    BitmapIndexFilterRows: 143.999M (143999213) // Bitmap 索引过滤掉的数据行数。
    BlockFetch: 8.988ms // 加载数据和 Bitmap 索引所需时间。相比于上个查询，虽然增加了加载 Bitmap 索引的时间，但是大大减少了加载数据的时间。
```
