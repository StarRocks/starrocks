---
displayed_sidebar: "Chinese"
---

# N-Gram Bloom Filter 索引

N-Gram Bloom Filter 索引是一种特殊的 [Bloom Filter 索引](./Bloomfilter_index.md)，只适用于字符串（`STRING`、`CHAR` 或 `VARCHAR`）类型的列。N-Gram Bloom Filter 与 Bloom Filter 的区别在于，N-Gram Bloom Filter 会根据对字符串进行分词，并将结果子串写入 Bloom Filter。

例如，如果字符串列中的某一行包含单词 `Technical`，传统的 Bloom Filter 会将整个字符串 `Technical` 写入 Bloom Filter。相比之下，指定了 `gram_num` 为 `4` 的 N-Gram Bloom Filter 会将 `Technical` 分词为以下子串：

```SQL
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

这六个子串中的每一个子串都会被写入 Bloom Filter。

## 使用说明

- 您可以为明细表或主键表的所有字符串列创建 N-Gram Bloom Filter 索引。对于聚合表或更新，只能为 Key 列创建 N-Gram Bloom Filter 索引。
- N-Gram Bloom Filter 索引可以用于以下数据类型的列：字符串类型（CHAR、STRING 和 VARCHAR）。
- 要确定查询是否命中 N-Gram Bloom Filter 索引，可以查看查询的 Profile 中的 `BloomFilterFilterRows` 字段。
- 对于单个列，只能创建 Bloom Filter 或 N-Gram Bloom Filter 其中一种类型的索引。

## 基本操作

### 创建 N-Gram Bloom Filter 索引

```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ("gram_num" = "4",
                                         "bloom_filter_fpp" = "0.05") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("replication_num"= "1");
```

索引相关参数：

| 参数               | 是否必须 | 描述                                                         |
| ------------------ | -------- | ------------------------------------------------------------ |
| `index_name`       | 是       | 索引的名称。索引名称必须在表内唯一。                         |
| `column_name`      | 是       | 创建索引的列名。只能指定一个列名。在上面的示例中，它是 `k2`。 |
| `gram_num`         | 是       | 对  N-Gram Bloom Filter 中的字符串列数据进行分词时的子串长度。在上面的示例中，`gram_num` 是 `4`。 |
| `bloom_filter_fpp` | 否       | Bloom Filter 的错误概率，范围为 0.0001 到 0.05。默认值为 0.05。较小的值提供更好的过滤效果，但会增加更大的存储开销。 |
| `case_sensitive`   | 否       | 此索引是否区分大小写。默认值为 `case_sensitive`。            |
| `COMMENT`          | 否       | 索引的注释。                                                 |

其他建表相关的参数解释，参见 [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。

## 查看 N-Gram Bloom Filter 索引

您可以使用 [SHOW CREATE TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md) 或 [SHOW INDEX](../../sql-reference/sql-statements/data-manipulation/SHOW_INDEX.md) 来查看表的所有索引。由于索引创建是异步的，因此只有在索引创建成功后才能看到相应的索引。

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

## 修改 N-Gram Bloom Filter 索引

您可以使用 [ALTER TABLE](../../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) 语句添加和删除 N-Gram Bloom Filter 索引。

以下语句将在表 `table1` 中为列 `k1` 添加一个新的 N-Gram Bloom Filter 索引，索引名称为 `new_index_name`。

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                            "bloom_filter_fpp" = "0.05") COMMENT '';
```

以下语句从表 `table1` 中删除名为 `new_index_name` 的 N-Gram Bloom Filter 索引。

```SQL
ALTER TABLE table1 DROP INDEX new_index_name;
```

:::note

修改索引是一个异步操作。您可以执行 [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md) 来查看此操作的进度。一次只能对表运行一个修改索引任务。

:::

## 支持加速的查询

### LIKE 查询

如果 `gram_num` 很小（小于查询字符串的长度），则可以在 LIKE 查询中使用 N-Gram Bloom Filter 索引，否则 N-Gram Bloom Filter 索引将不会加速 LIKE 查询。

例如：如果 `gram_num` 为 4，查询是 `SELECT * FROM table WHERE col1 LIKE "%abc"`，则对于该查询，N-Gram 索引不起作用，因为查询的是`abc`，只有三个字符。如果查询使用 `LIKE "%abcd" 或 LIKE "%abcde%"` 等，则 N-Gram Bloom Filter 索引会生效。

### ngram_search

当使用 `ngram_search` 函数时，如果查询的列有 N-Gram Bloom Filter 索引，并且在 `ngram_search` 函数中指定的 `gram_num` 与 N-Gram Bloom Filter 索引的 `gram_num` 匹配，则索引会自动把字符串相似度为 0 的数据过滤掉，显著加快了函数执行过程。

### ngram_search_case_insensitive

该函数的使用方式与 `ngram_search` 相同，但在创建 N-Gram Bloom Filter 索引时必须设置 `case_sensitive` 为 `false`：

```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ("gram_num" = "4",
                                         "bloom_filter_fpp" = "0.05",
                                         "case_sensitive" = "false") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("replication_num"= "1");
```

如果已经创建了索引，则可以修改索引的参数 `case_sensitive` 为 `false`：

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4",
                                            "bloom_filter_fpp" = "0.05",
                                            "case_sensitive" = "false") COMMENT '';
```
