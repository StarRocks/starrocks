---
displayed_sidebar: docs
toc_max_heading_level: 4
sidebar_position: 50
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# 全文倒排索引

<Beta />

从版本 3.3.0 开始，StarRocks 支持全文倒排索引，可以将文本拆分成更小的词，并为每个词创建一个索引条目，显示该词与数据文件中对应行号之间的映射关系。对于全文搜索，StarRocks 根据搜索关键词查询倒排索引，快速定位匹配关键词的数据行。

全文倒排索引支持多种底层实现。其中，Tantivy 实现支持明细表、主键表，以及存算一体和存算分离集群。

## 概述

StarRocks 将其底层数据存储在按列组织的数据文件中。每个数据文件包含基于索引列的全文倒排索引。索引列中的值被分词为单个词。分词后的每个词被视为一个索引条目，映射到该词出现的行号。目前支持的分词方法包括英文分词、中文分词、多语言分词和不分词。

例如，如果一行数据包含 "hello world" 且其行号为 123，全文倒排索引根据分词结果和行号构建索引条目：hello->123, world->123。

在全文搜索过程中，StarRocks 可以使用全文倒排索引定位包含搜索关键词的索引条目，然后快速找到关键词出现的行号，显著减少需要扫描的数据行数。

## 使用 Tantivy 倒排索引

### 创建索引

创建索引前，启用 FE 配置 `enable_experimental_gin`：

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

在索引属性中设置 `"imp_lib" = "tantivy"`。以下示例使用英文分词：

```sql
CREATE TABLE docs (
    id BIGINT NOT NULL,
    content VARCHAR(65535),
    INDEX idx_content (content) USING GIN (
        "imp_lib" = "tantivy",
        "parser" = "english"
    )
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replicated_storage" = "false"
);
```

也可以在建表后添加索引：

```sql
CREATE INDEX idx_content ON docs (content) USING GIN (
    "imp_lib" = "tantivy",
    "parser" = "english"
);
```

### 查询索引

Tantivy 支持以下常用查询：

```sql
-- 匹配任意分词
SELECT * FROM docs WHERE content MATCH_ANY 'database search';

-- 匹配全部分词。MATCH 在无通配符时也使用 AND 语义
SELECT * FROM docs WHERE content MATCH_ALL 'database search';

-- 短语匹配；~1 表示允许一个位置偏移
SELECT * FROM docs WHERE content MATCH_PHRASE 'full search ~1';

-- BM25 相关度排序
SELECT *, score()
FROM docs
WHERE content MATCH_ANY 'database search'
ORDER BY score() DESC
LIMIT 10;
```

`MATCH`、`MATCH_ANY`、`MATCH_ALL` 和 `MATCH_PHRASE` 必须作为索引列上的 `WHERE` 下推谓词。右侧通常为非空字符串字面量。`MATCH_ANY` 和 `MATCH_ALL` 也可以接收 `tokenize()` 的结果：

```sql
SELECT * FROM docs
WHERE content MATCH_ALL tokenize('english', 'Database Search');
```

系统变量 `enable_gin_filter` 默认为 `true`。如被关闭，需要重新启用：

```sql
SET enable_gin_filter = true;
```

### 支持的分词器

| `parser` | 说明 | 专属参数 |
| --- | --- | --- |
| `none` | 默认值。不分词，将整段文本作为一个词项。 | - |
| `english` | 英文分词，转为小写并移除英文停用词；不做词干提取。 | - |
| `standard` | 兼容 CLucene 语法的通用分词，支持邮箱、缩写等词项；转为小写并移除英文停用词。 | - |
| `chinese` / `cjk` | CJK 二元分词，生成相邻且重叠的双字词项；ASCII 字符转为小写。`cjk` 是 `chinese` 的别名。 | - |
| `jieba` | 基于 Jieba 词典的中文搜索模式分词；ASCII 字符转为小写。 | - |
| `ik` | 基于 IK 词典的中文分词。 | `parser_mode` |
| `ngram` | 按 Unicode 字符生成连续 N-Gram，并转为小写。 | `min_gram`、`max_gram` |

`english` 会忽略长度超过 40 个字符的词项。`standard` 的单个词项最长为 255 个字符。

可以用 `tokenize()` 查看分词结果。DDL 中 `parser = 'ik'` 对应的两种模式，在 `tokenize()` 中分别使用 `ik` 和 `ik_smart`；N-Gram 使用 `ngram:<min_gram>:<max_gram>`：

```sql
SELECT tokenize('ik', '中华人民共和国国歌');
SELECT tokenize('ik_smart', '中华人民共和国国歌');
SELECT tokenize('ngram:2:3', 'Ab中');
```

### 索引参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `imp_lib` | - | 使用 Tantivy 时必须设为 `tantivy`。在存算分离集群中也必须显式指定。 |
| `parser` | `none` | 指定分词器。 |
| `parser_mode` | `ik_max_word` | 仅用于 `parser = 'ik'`。支持 `ik_max_word`（细粒度）和 `ik_smart`（粗粒度）。 |
| `min_gram` | - | 仅用于 `parser = 'ngram'`。必须与 `max_gram` 同时设置，且为正整数。 |
| `max_gram` | - | 仅用于 `parser = 'ngram'`。必须大于等于 `min_gram`。 |
| `support_phrase` | `true` | 是否保存词项位置信息。使用 `MATCH_PHRASE` 时必须为 `true`。 |
| `support_bm25` | `true` | 是否保存 BM25 所需的 fieldnorm。使用 `score()` 时必须为 `true`。 |

`support_phrase` 和 `support_bm25` 仅适用于 Tantivy，并在建索引时生效。修改后需要重建索引。

### 使用限制

- 每个倒排索引只能包含一个 `CHAR`、`VARCHAR` 或 `STRING` 列。
- 仅支持明细表和主键表。主键表支持全行写入和行模式部分更新，不支持列模式部分更新。
- 在存算一体集群中通过 `ALTER TABLE` 或 `CREATE INDEX` 添加索引时，表属性 `replicated_storage` 必须为 `false`。
- 通配符查询支持 `%` 和 `*`，但通配符表达式不会经过分词。它直接匹配索引中的词项，因此需要考虑分词器的切分和大小写规则。
- `MATCH_PHRASE` 的偏移量写在查询文本末尾，格式为 `'text ~N'`。`~` 前必须有空格，`N` 为非负整数。
- `score()` 仅用于非取反的 `MATCH`、`MATCH_ANY` 或 `MATCH_ALL` 查询，不适用于 `MATCH_PHRASE` 和通配符查询。

## 基本操作

### 创建全文倒排索引

在创建全文倒排索引之前，需要启用 FE 配置项 `enable_experimental_gin`。

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

全文倒排索引可以在明细表或主键表中创建。使用限制取决于底层实现；Tantivy 的限制请参见[使用限制](#使用限制)。

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

执行 `ALTER TABLE DROP INDEX` 或 `DROP INDEX` 删除全文倒排索引。

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
