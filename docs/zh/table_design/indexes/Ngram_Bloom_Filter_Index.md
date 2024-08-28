---
displayed_sidebar: docs
---

# [Preview] N-Gram bloom filter 索引

N-Gram bloom filter 索引是一种特殊的 [Bloom filter 索引](./Bloomfilter_index.md)，通常用于加速 LIKE 查询或 `ngram_search` 和 `ngram_search_case_insensitive` 函数的运算速度。

N-Gram bloom filter 索引只适用于字符串（`STRING`、`CHAR` 或 `VARCHAR`）类型的列。N-Gram bloom filter 索引与 Bloom filter 索引的区别在于，N-Gram bloom filter 索引会先根据对字符串进行分词，然后将结果子串写入 Bloom filter 索引。例如某一个索引列值为字符串 `Technical`，如果是传统的 Bloom filter 索引，则将整个字符串 `Technical` 直接写入索引中。然而如果是 N-Gram bloom filter 索引，并且 `gram_num` 指定为 `4`，则需要先对 `Technical` 进行分词处理为以下子串：

```SQL
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

然后将这六个子串中的每一个子串写入 Bloom filter 索引。

## 使用说明

- 对于明细表或主键表，您可以为所有列（字符串类型）创建 N-Gram bloom filter 索引。对于聚合表或更新表，您只能为 Key 列（字符串类型）创建 N-Gram bloom filter 索引。
- N-Gram bloom filter 索引只适用于字符串类型（CHAR、STRING 和 VARCHAR）的列。
- 要确定查询是否命中 N-Gram bloom filter 索引，可以查看查询的 Profile 中的 `BloomFilterFilterRows` 字段。
- 对于单个列，只能创建 Bloom filter 或 N-Gram bloom filter 其中一种类型的索引。

## 基本操作

### 创建 N-Gram bloom filter 索引

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

N-Gram bloom filter 索引相关参数：

| 参数               | 是否必须 | 描述                                                         |
| ------------------ | -------- | ------------------------------------------------------------ |
| `index_name`       | 是       | 索引的名称。索引名称必须在表内唯一。                         |
| `column_name`      | 是       | 创建索引的列名。只能指定一个列名。在上面的示例中，索引的列名为 `k2`。 |
| `gram_num`         | 否       | 索引列的字符串进行分词后的子串长度。默认值为2。 |
| `bloom_filter_fpp` | 否       | Bloom filter 的错误概率，范围为 0.0001 到 0.05。默认值为 0.05。较小的值提供更好的过滤效果，但会增加更大的存储开销。 |
| `case_sensitive`   | 否       | 此索引是否区分大小写。默认值为 `true`。            |
| `COMMENT`          | 否       | 索引的注释。                                                 |

其他建表相关的参数解释，参见 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

### 查看 N-Gram bloom filter 索引

您可以使用 [SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) 或 [SHOW INDEX](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_INDEX.md) 来查看表的所有索引。由于索引创建是异步的，因此只有在成功创建索引后才能看到相应的索引。

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

### 修改 N-Gram bloom filter 索引

您可以使用 [ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 语句添加和删除 N-Gram bloom filter 索引。

- 执行如下语句，在表 `table1` 中为列 `k1` 添加一个新的 N-Gram bloom filter 索引，索引名称为 `new_index_name`。

    ```SQL
    ALTER TABLE table1 
    ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                                "bloom_filter_fpp" = "0.05") COMMENT '';
    ```

- 执行如下语句，在表 `table1` 中删除名为 `new_index_name` 的 N-Gram bloom filter 索引。

    ```SQL
    ALTER TABLE table1 DROP INDEX new_index_name;
    ```

:::note

修改索引是一个异步操作。您可以执行 [SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) 来查看此操作的进度。一次只能对表运行一个修改索引任务。

:::

## 支持加速的查询

### LIKE 查询

当 `gram_num` 很小（小于所查询的字符串长度），则 LIKE 查询可以利用 N-Gram bloom filter 索引进行加速。反之，则 LIKE 查询则无法利用 N-Gram bloom filter 索引进行加速。

例如 `gram_num` 为 `4`，查询语句为 `SELECT * FROM table WHERE col1 LIKE "%abc"`，则 N-Gram bloom filter 索引不会加速该查询，因为所查询的字符串是 `abc`，只有三个字符，小于 `gram_num` 的值 `4`。如果查询条件为 `WHERE col1 LIKE "%abcd"` 或 `WHERE col1 LIKE "%abcde%"`，则 N-Gram bloom filter 索引会加速该查询。

### ngram_search

当查询中使用了 `ngram_search` 函数，如果基于函数中的列，表中已经创建 N-Gram bloom filter 索引，并且在函数中指定的 `gram_num` 与 N-Gram bloom filter 索引的 `gram_num` 匹配，则索引会自动把字符串相似度为 0 的数据过滤掉，显著加快函数执行过程。

### ngram_search_case_insensitive

除了该函数对小大写敏感外之外，该函数的使用方式与 `ngram_search` 相同。在创建 N-Gram bloom filter 索引时必须设置 `case_sensitive` 为 `false`：

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

如果已经创建了索引，且索引的参数 `case_sensitive` 为 `true`，则可以修改该参数为 `false`：

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4",
                                            "bloom_filter_fpp" = "0.05",
                                            "case_sensitive" = "false") COMMENT '';
```
