---
displayed_sidebar: docs
sidebar_position: 40
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# N-gram bloom filter 索引

<Beta />

N-gram bloom filter 索引是一种特殊的 [Bloom filter 索引](./Bloomfilter_index.md)，通常用于加速 LIKE 查询以及 `ngram_search` 和 `ngram_search_case_insensitive` 函数的计算速度。

N-gram bloom filter 索引仅适用于字符串类型（`STRING`、`CHAR` 或 `VARCHAR`）的列。N-gram bloom filter 索引与 Bloom filter 索引的区别在于，N-gram bloom filter 索引首先对字符串进行分词，然后将生成的子字符串写入 Bloom filter 索引。例如，某个被索引的列值是字符串 `Technical`。对于传统的 Bloom filter 索引，整个字符串 `Technical` 将直接写入索引。然而，对于指定 `gram_num` 为 `4` 的 N-gram bloom filter 索引，字符串 `Technical` 将首先被分词为以下子字符串：

```sql
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

然后将这六个子字符串分别写入 Bloom filter 索引。

## 使用注意事项

- 对于 Duplicate Key 或 Primary Key 表，可以为所有列（字符串类型）创建 N-gram bloom filter 索引。对于聚合表或更新表，只能在键列（字符串类型）上创建 N-gram bloom filter 索引。
- N-gram bloom filter 索引只能为字符串类型的列（CHAR、STRING 和 VARCHAR）创建。
- 要确定查询是否命中 N-gram bloom filter 索引，可以检查查询的 `BloomFilterFilterRows` 字段。
- 对于单个列，只能创建一种类型的索引（Bloom Filter 或 N-gram Bloom Filter）。

## 基本操作

### 创建 N-gram bloom filter 索引

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
DISTRIBUTED BY HASH(k1);

```

N-gram bloom filter 索引相关参数：

| **参数**          | **必需**   | **描述**                                                      |
| ---------------- | ---------- | ------------------------------------------------------------ |
| `index_name`       | 是         | 索引的名称。索引名称在表内必须唯一。                          |
| `column_name`      | 是         | 创建索引的列名。只能指定一个列名。在上面的例子中，它是 `k2`。 |
| `gram_num`         | 否         | 被索引列中的字符串分词后的子字符串长度。默认值为 2。          |
| `bloom_filter_fpp` | 否         | Bloom filter 的误报率，范围从 0.0001 到 0.05。默认值为 0.05。较小的值提供更好的过滤效果，但会增加存储开销。 |
| `case_sensitive`   | 否         | 此索引是否区分大小写。默认值为 `true`。                      |
| `COMMENT`          | 否         | 索引注释。                                                   |

有关表创建的其他参数说明，请参见 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

### 查看 N-gram bloom filter 索引

您可以使用 `SHOW CREATE TABLE` 或 `SHOW INDEX FROM` 表来查看表的所有索引。由于索引创建是异步的，只有在索引成功创建后才能看到相应的索引。

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

### 修改 N-gram bloom filter 索引

您可以使用 [ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 语句添加和删除 N-gram bloom filter 索引。

- 执行以下语句为表 `table1` 的列 `k1` 添加新的 N-gram bloom filter 索引 `new_index_name`。

  ```SQL
  ALTER TABLE table1 
  ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                              "bloom_filter_fpp" = "0.05") COMMENT '';
  ```

- 执行以下语句从表 `table1` 中删除 N-gram bloom filter 索引 `new_index_name`。

  ```SQL
  ALTER TABLE table1 DROP INDEX new_index_name;
  ```

:::note

修改索引是一个异步操作。您可以通过执行 [SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) 查看此操作的进度。每次只能运行一个任务来修改表中的索引。

:::

## 可以加速的查询

### `LIKE` 查询

如果 `gram_num` 足够小（小于被查询字符串的长度），N-gram bloom filter 索引可以加速 `LIKE` 查询。否则，N-gram bloom filter 索引无法加速 `LIKE` 查询。

例如，如果 `gram_num` 是 `4`，查询语句是 `SELECT * FROM table WHERE col1 LIKE "%abc"`，则 N-Gram Bloom filter 索引不会加速此查询，因为查询的字符串是 `abc`，只有三个字符，小于 `gram_num` 的值 `4`。如果查询条件是 `WHERE col1 LIKE "%abcd"` 或 `WHERE col1 LIKE "%abcde%"`，则 N-Gram Bloom filter 索引将加速查询。

### ngram_search

当查询中使用 `ngram_search` 函数时，如果函数中指定的列具有 N-gram bloom filter 索引，并且函数中指定的 `gram_num` 与 N-gram bloom filter 索引的 `gram_num` 匹配，则索引将自动过滤掉字符串相似度为 0 的数据，从而显著加快函数执行过程。

### ngram_search_case_insensitive

此函数的用法与 `ngram_search` 相同，只是此函数区分大小写。因此，当创建 N-gram bloom filter 索引时，需要将 `case_sensitive` 设置为 `false`。

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
DISTRIBUTED BY HASH(k1);
```

如果索引已经创建并且其参数 `case_sensitive` 设置为 `true`，可以通过以下方式将此索引的参数更改为 `false`：

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                            "bloom_filter_fpp" = "0.05",
                                            "case_sensitive" = "false") COMMENT '';
```