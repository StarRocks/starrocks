---
displayed_sidebar: "Chinese"
---

# Bloom filter 索引

本文介绍了 Bloom filter（布隆过滤器）索引的原理，以及如何创建和修改 Bloom filter 索引。

Bloom filter 索引可以快速判断表的数据文件中是否可能包含要查询的数据，如果不包含就跳过，从而减少扫描的数据量。Bloom filter 索引空间效率高，适用于基数较高的列，如 ID 列。如果一个查询条件命中前缀索引列，StarRocks 会使用[前缀索引](../table_design/Sort_key.md)快速返回查询结果。但是前缀索引的长度有限，如果想要快速查询一个非前缀索引列且该列基数较高，即可为这个列创建 Bloom filter 索引。

## 索引原理

例如，在表 `table1` 的 `column1` 列上创建 Bloom filter 索引，然后执行一个 SQL 查询 `Select xxx from table1 where column1 = something;` 命中索引，那么在扫描表的数据文件时会出现两种情况：

- 如果 Bloom filter 索引判断一个数据文件中不存在目标数据，那 StarRocks 会跳过该文件，从而提高查询效率。
- 如果 Bloom filter 索引判断一个数据文件中可能存在目标数据，那 StarRocks 会读取该文件确认目标数据是否存在。注意，这里仅仅是判断该文件中可能包含目标数据。Bloom filter 索引有一定的误判率，也称为假阳性概率 (False Positive Probability)，即判断某行中包含目标数据，而实际上该行并不包含目标数据的概率。

## 使用说明

- 主键表和明细表中所有列都可以创建 Bloom filter 索引；聚合表和更新表中，只有维度列（即 Key 列）支持创建 Bloom filter 索引。
- 不支持为 TINYINT、FLOAT、DOUBLE 和 DECIMAL 类型的列创建 Bloom filter 索引。
- Bloom filter 索引只能提高包含 `in` 和 `=` 过滤条件的查询效率，例如 `Select xxx from table where xxx in ()` 和 `Select xxx from table where column = xxx`。
- 如要了解一个查询是否命中了 Bloom filter 索引，可查看该查询的 Profile 中的 `BloomFilterFilterRows` 字段。关于如何查看 Profile，参见[分析查询](../administration/Query_planning.md#查看分析-profile)。

## 创建索引

建表时，通过在 `PROPERTIES` 中指定 `bloom_filter_columns` 来创建索引。例如，如下语句为表 `table1` 的 `k1` 和 `k2` 列创建 Bloom filter 索引。

```SQL
CREATE TABLE table1
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT DEFAULT "10"
)
ENGINE = olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES("bloom_filter_columns" = "k1,k2");
```

您可以同时创建多个索引，多个索引列之间需用逗号 (`,`) 隔开。关于建表的其他参数说明，请参见 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。

## 查看索引

例如，查看表 `table1` 的索引。有关返回值说明，请参见 [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md)。

```SQL
SHOW CREATE TABLE table1;
```

## 修改索引

您可以使用 [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) 语句来增加，减少和删除索引。

- 如下语句增加了一个 Bloom filter 索引列 `v1`。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1,k2,v1");
    ```

- 如下语句减少了一个 Bloom filter 索引列 `k2`。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1");
    ```

- 如下语句删除了 `table1` 的所有 Bloom filter 索引。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "");
    ```

> 说明：修改索引为异步操作，可通过 [SHOW ALTER TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md) 命令查看索引修改进度。当前每张表只允许同时进行一个修改索引任务。
