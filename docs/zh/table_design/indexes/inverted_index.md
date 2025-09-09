---
displayed_sidebar: docs
toc_max_heading_level: 4
sidebar_position: 50
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# 全文倒排索引

<Beta />

从版本 3.3.0 开始，StarRocks 支持全文倒排索引，可以将文本拆分成更小的词，并为每个词创建一个索引条目，显示该词与数据文件中对应行号之间的映射关系。对于全文搜索，StarRocks 根据搜索关键词查询倒排索引，快速定位匹配关键词的数据行。

主键表自 v4.0 起支持全文倒排索引。

全文倒排索引尚不支持存算分离集群。

## 概述

StarRocks 将其底层数据存储在按列组织的数据文件中。每个数据文件包含基于索引列的全文倒排索引。索引列中的值被分词为单个词。分词后的每个词被视为一个索引条目，映射到该词出现的行号。目前支持的分词方法包括英文分词、中文分词、多语言分词和不分词。

例如，如果一行数据包含 "hello world" 且其行号为 123，全文倒排索引根据分词结果和行号构建索引条目：hello->123, world->123。

在全文搜索过程中，StarRocks 可以使用全文倒排索引定位包含搜索关键词的索引条目，然后快速找到关键词出现的行号，显著减少需要扫描的数据行数。

## 基本操作

### 创建全文倒排索引

在创建全文倒排索引之前，需要启用 FE 配置项 `enable_experimental_gin`。

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

:::note
在为表创建全文倒排索引时，必须禁用该表的 `replicated_storage` 功能。
- 对于 v4.0 及更高版本，创建索引时该功能会自动禁用。
- 对于 v4.0 之前的版本，必须手动将表属性 `replicated_storage` 设置为 `false`。
:::

#### 在创建表时创建全文倒排索引

在列 `v` 上创建使用英文分词的全文倒排索引。

```SQL
CREATE TABLE `t` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` STRING COMMENT "",
   INDEX idx (v) USING GIN("parser" = "english")
) ENGINE=OLAP 
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replicated_storage" = "false"
);
```

- `parser` 参数指定分词方法。支持的值及描述如下：
  - `none` (默认): 不分词。构建全文倒排索引时，索引列中的整行数据被视为单个索引项。
  - `english`: 英文分词。此分词方法通常在任何非字母字符处进行分词。此外，大写英文字符会被转换为小写。因此，查询条件中的关键词需要是小写英文而不是大写英文，以利用全文倒排索引定位数据行。
  - `chinese`: 中文分词。此分词方法使用 CLucene 中的 [CJK Analyzer](https://lucene.apache.org/core/6_6_1/analyzers-common/org/apache/lucene/analysis/cjk/package-summary.html) 进行分词。
  - `standard`: 多语言分词。此分词方法提供基于语法的分词（基于 [Unicode Text Segmentation algorithm](https://unicode.org/reports/tr29/)），适用于大多数语言和混合语言的情况，如中英文。例如，此分词方法可以区分中英文。当中英文共存时，分词后会将大写英文字符转换为小写。因此，查询条件中的关键词需要是小写英文而不是大写英文，以利用全文倒排索引定位数据行。
- 索引列的数据类型必须是 CHAR、VARCHAR 或 STRING。

#### 在创建表后添加全文倒排索引

在创建表后，可以使用 `ALTER TABLE ADD INDEX` 或 `CREATE INDEX` 添加全文倒排索引。

```SQL
ALTER TABLE t ADD INDEX idx (v) USING GIN('parser' = 'english');
CREATE INDEX idx ON t (v) USING GIN('parser' = 'english');
```

### 管理全文倒排索引

#### 查看全文倒排索引

执行 `SHOW CREATE TABLE` 查看全文倒排索引。

```SQL
MySQL [example_db]> SHOW CREATE TABLE t\G
```

#### 删除全文倒排索引

执行 `ALTER TABLE ADD INDEX` 或 `DROP INDEX` 删除全文倒排索引。

```SQL
DROP INDEX idx on t;
ALTER TABLE t DROP index idx;
```

### 通过全文倒排索引加速查询

创建全文倒排索引后，需要确保系统变量 `enable_gin_filter` 已启用，以便倒排索引能够加速查询。此外，还需考虑索引列值是否已分词，以确定哪些查询可以加速。

#### 当索引列已分词时支持的查询

当全文倒排索引列启用分词（`parser` = `standard` | `english` | `chinese`）时，仅支持使用 `MATCH`、`MATCH_ANY` 或 `MATCH_ALL` 谓词进行过滤，格式为：
- `<col_name> (NOT) MATCH '%keyword%'`
- `<col_name> (NOT) MATCH_ANY 'keyword1, keyword2'`
- `<col_name> (NOT) MATCH_ALL 'keyword1, keyword2'`

其中，keyword 必须为字符串字面量，不支持表达式。
1. 创建一个表并插入几行测试数据。

      ```SQL
      CREATE TABLE `t` (
          `id1` bigint(20) NOT NULL COMMENT "",
          `value` varchar(255) NOT NULL COMMENT "",
          INDEX gin_english (`value`) USING GIN ("parser" = "english") COMMENT 'english index'
      ) 
      DUPLICATE KEY(`id1`)
      DISTRIBUTED BY HASH(`id1`)
      PROPERTIES (
      "replicated_storage" = "false"
      );
      
      
      INSERT INTO t VALUES
          (1, "starrocks is a database
      
      1"),
          (2, "starrocks is a data warehouse");
      ```

2. 使用 `MATCH` 谓词进行查询。

- 查询 `value` 列包含关键词 `starrocks` 的数据行。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks";
    ```

- 检索 `value` 列包含以 `data` 开头的关键词的数据行。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "data%";
    ```

3. 使用 `MATCH_ANY` 谓词进行查询。

- 查询 `value` 列包含关键词 `database` 或者包含 `data`的数据行。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH_ANY "database data";
    ```
4. 使用 `MATCH_ALL` 谓词进行查询。

- 查询 `value` 列既包含关键词 `database` 又包含 `data`的数据行。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH_ALL "database data";
    ```
  
**注意：**

- 在查询过程中，`MATCH`可以使用 `%` 进行模糊匹配，格式为 `%keyword%`。但关键词必须包含单词的一部分。例如，如果关键词是 <code>starrocks&nbsp;</code>，则无法匹配单词 `starrocks`，因为它包含空格。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "star%";
    +------+-------------------------------+
    | id1  | value                         |
    +------+-------------------------------+
    |    1 | starrocks is a database1      |
    |    2 | starrocks is a data warehouse |
    +------+-------------------------------+
    2 rows in set (0.02 sec)
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks ";
    Empty set (0.02 sec)
    ```

- 如果使用英文或多语言分词构建全文倒排索引，存储时会将大写英文单词转换为小写。因此，在使用`MATCH`查询时，关键词需要是小写而不是大写，以利用全文倒排索引定位数据行。

    ```SQL
    MySQL [example_db]> INSERT INTO t VALUES (3, "StarRocks is the BEST");
    
    MySQL [example_db]> SELECT * FROM t;
    +------+-------------------------------+
    | id1  | value                         |
    +------+-------------------------------+
    |    1 | starrocks is a database       |
    |    2 | starrocks is a data warehouse |
    |    3 | StarRocks is the BEST         |
    +------+-------------------------------+
    3 rows in set (0.02 sec)
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "BEST"; -- 关键词为大写英文
    Empty set (0.02 sec) -- 返回空结果集
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "best"; -- 关键词为小写英文
    +------+-----------------------+
    | id1  | value                 |
    +------+-----------------------+
    |    3 | StarRocks is the BEST | -- 能定位到符合条件的数据行
    +------+-----------------------+
    1 row in set (0.01 sec)
    ```

  - 查询条件中的 `MATCH` 、`MATCH_ANY`或`MATCH_ALL`谓词必须用作下推谓词，因此必须在 WHERE 子句中并针对索引列执行。

      以以下表和测试数据为例：

      ```SQL
      CREATE TABLE `t_match` (
          `id1` bigint(20) NOT NULL COMMENT "",
          `value` varchar(255) NOT NULL COMMENT "",
          `value_test` varchar(255) NOT NULL COMMENT "",
          INDEX gin_english (`value`) USING GIN("parser" = "english") COMMENT 'english index'
      )
      ENGINE=OLAP 
      DUPLICATE KEY(`id1`)
      DISTRIBUTED BY HASH (`id1`) BUCKETS 1 
      PROPERTIES (
      "replicated_storage" = "false"
      );
    
      INSERT INTO t_match VALUES (1, "test", "test");
      ```

      以下查询语句不符合要求：

      - 因为查询语句中的 `MATCH` 、`MATCH_ANY`或`MATCH_ALL`谓词不在 WHERE 子句中，无法下推，导致查询错误。

          ```SQL
          MySQL [test]> SELECT value MATCH "test" FROM t_match;
          ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
          ```

      - 因为查询语句中 `MATCH`、`MATCH_ANY`或`MATCH_ALL` 谓词执行的列 `value_test` 不是索引列，查询失败。

          ```SQL
          MySQL [test]> SELECT * FROM t_match WHERE value_test match "test";
          ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
          ```

#### 当索引列未分词时支持的查询

如果全文倒排索引未对索引列进行分词，即 `'parser' = 'none'`，则查询条件中列出的所有下推谓词均可用于使用全文倒排索引进行数据过滤：

- 表达式谓词: (NOT) LIKE, (NOT) MATCH，(NOT) MATCH_ANY，(NOT) MATCH_ALL
  
  :::note

  - 在这种情况下，`MATCH` 在语义上等同于 `LIKE`。
  - `MATCH` 和 `LIKE` 仅支持格式 `(NOT) <col_name> MATCH|LIKE '%keyword%'`。`keyword` 必须是字符串字面量，不支持表达式。注意，如果 `LIKE` 不符合此格式，即使查询可以正常执行，也会降级为不使用全文倒排索引过滤数据的查询。
  :::
- 常规谓词: `==`, `!=`, `<=`, `>=`, `NOT IN`, `IN`, `IS NOT NULL`, `NOT NULL`

## 如何验证全文倒排索引是否加速查询

执行查询后，可以在 Query Profile 的扫描节点中查看详细指标 `GinFilterRows` 和 `GinFilter`，以查看使用全文倒排索引过滤的行数和过滤时间。