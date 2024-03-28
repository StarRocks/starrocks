---
displayed_sidebar: "Chinese"
---

# Ngram Bloom filter 索引

关于Bloom filter（布隆过滤器）索引的原理，以及如何创建和修改 Bloom filter 索引,可以参考[布隆过滤器](./Bloomfilter_index.md)。Ngram Bloom filter索引是特殊的Bloom filter索引, 只适用于列为字符串类型的情况。Ngram Bloom filter和Bloom filter的区别是, Ngram Bloom filter在创建索引时, 会将字符串根据用户提供的gram_num进行分词, 将分词后的字符串子串写入Bloom filter中。

例如当字符串列中某一行为"Technical"时且, Bloom filter会将"Technical"整个字符串写入Bloom filter中, 而指定gram_num为4的Ngram Bloom filter则会先将"Technical"分词为:

```
"Tech","echn","chni","hnic","nica","ical"
```

然后将这六个字符串子串分别写入Bloom filter中。

## 使用说明

- 主键表和明细表中所有列都可以创建Ngram Bloom filter 索引；聚合表和更新表中，只有维度列（即 Key 列）支持创建Ngram Bloom filter 索引。
- 支持为如下类型的列创建Ngram Bloom filter 索引。
  - 字符串类型：CHAR、STRING 和 VARCHAR。
- 如要了解一个查询是否命中了Ngram Bloom filter 索引，可查看该查询的 Profile 中的 `BloomFilterFilterRows` 字段。关于如何查看 Profile，参见[分析查询](../../administration/Query_planning.md#查看分析-profile)。
- 同一列只支持创建Bloom filter索引或者Ngram Bloom filter索引, 不支持同时创建两种索引。

## 创建索引

```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.05") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("replication_num"= "1");
```

- 其中有关索引部分参数说明如下：

  | **参数**         | **必选** | **说明**                                                     |
  | ---------------- | -------- | ------------------------------------------------------------ |
  | index_name       | 是       | 索引名称。命名要求参见[系统限制](../../reference/System_limit.md)。在同一张表中不能创建名称相同的索引。 |
  | column_name      | 是       | 创建索引的列名。只能指定单个列名, 上述例子中为"k2"。         |
  | gram_num         | 是       | Ngram bloom filter对字符串列的一行数据分词时, 字符串子串的长度。上述例子中gram_num为4。 |
  | bloom_filter_fpp | 否       | flase positive possibility, 即bloom filter的假阳率, 取值在[0.0001,0.05]。默认为0.05, 值越小, 过滤效果越好, 但是存储开销越大。 |
  | case_sensitive   | 否       | 该索引是否大小写敏感.默认大小写敏感|
  | COMMENT          | 否       | 索引备注。                                                   |

  关于建表的其他参数说明，参见 [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。

## 查看索引

可以通过 show create table或者show index from table 查看该表的所有index，因为创建索引是异步的, 因此只有创建索引成功后才能通过show命令看到对应的索引。

```SQL
SHOW CREATE table table1;
show index from table1;
```

## 修改索引

在创建表后, 您可以使用 [ALTER TABLE](../../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) 语句来增加和删除索引。

- 如下语句为表table1增加了一个Ngram Bloom filter 索引列 `k1`, 该索引名为new_index_name。

    ```SQL
    ALTER TABLE table1 ADD INDEX new_index_name(k1) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.05") COMMENT ''
    ```

- 如下语句删除了表table1中名为new_index_name的Ngram Bloom filter 索引。

    ```SQL
    ALTER TABLE table1 DROP INDEX new_index_name;
    ```

> 说明：修改索引为异步操作，可通过 [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md) 命令查看索引修改进度。当前每张表只允许同时进行一个修改索引任务。

# 函数支持

## LIKE
如果索引的 "gram_num" 足够小，Ngram 布隆过滤器索引可以用于 LIKE 查询加速，否则不能加速相关的LIKE查询。例如：如果gram_num为 ，但查询是 `select * from table where col1 like "%abc"`，因为 "abc" 只有三个字符，所以该索引对于这个查询是无效的。但如果查询是类似 "%abcd" 或者 "%abcde%" 这样的，那么该索引可以过滤掉大量数据。

## ngram_search
在使用 `ngram_search` 函数时，如果查询的列具有 Ngram 布隆过滤器索引，并且在 `ngram_search` 函数中指定的 `gram_num` 与 Ngram 布隆过滤器索引的 `gram_num` 相同，索引将自动过滤掉字符串相似度为 0 的数据，加快函数执行过程。

## ngram_search_case_insensitive
与 `ngram_search` 类似，但在为不区分大小写的情况下创建 Ngram 布隆过滤器索引时，需要按照以下方式创建索引：
```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.05", "case_sensitive" = "false") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("replication_num"= "1");
```
或者如果已经创建了索引，则修改索引：
```SQL
ALTER TABLE table1 ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05","case_sensitive" = "false") COMMENT '';
```
