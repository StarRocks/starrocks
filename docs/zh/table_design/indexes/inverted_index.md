---
displayed_sidebar: docs
keywords: ['suoyin']
toc_max_heading_level: 4
sidebar_position: 50
---

# [Preview] 全文倒排索引

自 3.3.0 起，StarRocks 支持全文倒排索引，将文本中的每个单词进行分词处理，并为每个词建立一个索引项，记录该单词与其所在数据文件行号的映射关系。进行全文检索时，StarRocks 会根据查询的关键词对倒排索引进行查询，快速定位到与关键词匹配的数据行。

StarRocks 存算分离集群以及主键表暂时不支持全文倒排索引。

## 功能简介

StarRocks 底层数据是按列存储在数据文件。每个数据文件会包含基于索引列的全文倒排索引。索引列的值会被分割为一个一个词，然后每个分词后的词语都会被视为一个索引项，与包含该词语的数据文件行号构成映射关系。目前支持的分词方式有英文分词、中文分词、多语言分词、不分词。

比如有一行数据 `hello world` 在数据文件的行号是 123，分词成 `hello` 和 `world` 两个词。全文倒排索引就会根据这个分词结果和行号构建索引项：hello->123, world->123。

进行全文检索时，借助全文倒排索引，StarRocks 可以根据查询条件中的关键词，找到包含关键词的索引项，然后快速找到关键词所在数据文件行号，大幅减少需要扫描的数据行数。

## 基本操作

### 创建全文倒排索引

创建全文倒排索引前，需要开启 FE 配置项 `enable_experimental_gin`。

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

并且创建时，仅支持表的类型为明细表并且必须设置表属性 `replicated_storage` 为 `false`。

#### 建表时创建全文倒排索引

基于列 `v` 构建全文倒排索引并且使用英文分词。

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

- `parser` 表示分词方式，支持的取值和描述如下：
  - `none`（默认）：不分词，索引列的整行数据作为一个索引项构建全文倒排索引。
  - `english`：英文分词，通常在任何非字母的字符分词。并且英文进行分词处理后，大写英文会转换为小写英文。因此查询时条件中关键词需要为小写英文而不是大写英文，才能借助全文倒排索引定位到数据行。
  - `chinese`：中文分词，使用 [CJK Analyzer](https://lucene.apache.org/core/6_6_1/analyzers-common/org/apache/lucene/analysis/cjk/package-summary.html) 进行分词处理。
  - `standard`：多语言分词。基于语法进行分词（基于 Unicode 文本分割算法，如 [Unicode 标准附录 #29](https://unicode.org/reports/tr29/) 中所指定的），适用于大多数语言，以及多语言混合的情况，比如中英文混合。例如中英文同时存在时，支持区分中英文，英文进行分词处理后，同时大写英文转小写，因此查询时条件中关键词需要为小写英文而不是大写英文，才能借助全文倒排索引定位到数据行。
- 索引列的数据类型必须是 CHAR、VARCHAR、STRING。

#### 建表后添加全文倒排索引

建表后，可以通过 `ALTER TABLE ADD INDEX` 或者 `CREATE INDEX` 添加全文倒排索引。

```SQL
ALTER TABLE t ADD INDEX idx (v) USING GIN('parser' = 'english');

CREATE INDEX idx ON t (v) USING GIN('parser' = 'english');
```

### 管理全文倒排索引

#### 查看全文倒排索引

执行 `SHOW CREATE TABLE` 查看全文倒排索引。

```SQL
MySQL [example_db]> SHOW CREATE TABLE t\G
*************************** 1. row ***************************
       Table: t
Create Table: CREATE TABLE `t` (
  `k` bigint(20) NOT NULL COMMENT "",
  `v` varchar(65533) NULL COMMENT "",
  INDEX idx (`v`) USING GIN("parser" = "english") COMMENT '' -- 全文倒排索引
) ENGINE=OLAP 
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "false",
"replication_num" = "3"
);
1 row in set (0.01 sec)
```

#### 删除全文倒排索引

执行 `ALTER TABLE ADD INDEX`或者 `DROP INDEX` 删除全文倒排索引。

```SQL
DROP INDEX idx on t;
ALTER TABLE t DROP index idx;
```

### 借助全文倒排索引进行查询

创建了全文倒排索引之后，您需要确保开启系统变量 `enable_gin_filter`，才能利用倒排索引加速查询。并且，您还需要根据索引列值是否进行分词处理来确定哪些查询能够利用索引加速。

#### 索引列进行分词时支持的查询

如果全文倒排索引对索引列进行分词处理，即 `'parser' = 'standard|english|chinese'` ，此时查询条件中仅支持谓词 `MATCH` 使用全文倒排索引进行数据过滤，并且格式必须为 `<col_name> (NOT) MATCH '%keyword%'`。其中 `keyword` 必须是字符串字面量，不支持为表达式。

1. 创建表并插入几行测试数据。

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
          (1, "starrocks is a database1"),
          (2, "starrocks is a data warehouse");
      ```

2. 使用谓词 `MATCH` 进行查询。

   1. 查询 `value` 列中包含关键词 `starrocks` 的数据行。

        ```SQL
        MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks";
        +------+-------------------------------+
        | id1  | value                         |
        +------+-------------------------------+
        |    1 | starrocks is a database       |
        |    2 | starrocks is a data warehouse |
        +------+-------------------------------+
        2 rows in set (0.05 sec)
        ```

   2. 查询 `value` 列中包含 `data` 开头的关键词的数据行。

        ```SQL
        MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "data%";
        +------+-------------------------------+
        | id1  | value                         |
        +------+-------------------------------+
        |    1 | starrocks is a database       |
        |    2 | starrocks is a data warehouse |
        +------+-------------------------------+
        2 rows in set (0.02 sec)
        ```

**注意事项**

- 查询时关键词可以使用 `%` 进行模糊匹配，格式支持为 `%keyword%`。但是关键词必须要包含某个词的一部分，如果关键词为 <code>starrocks&nbsp;</code>，因为其中包含空格，则无法匹配词 `starrocks`。

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

- 如果构建全文倒排索引时使用英文分词或者多语言分词，即使词是大写英文，实际存储在全文倒排索引中大写英文会转成小写英文。因此查询时关键词需要为小写而不是大写，才能借助全文倒排索引定位到数据行。

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
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "BEST"; ---关键词为大写英文
    Empty set (0.02 sec) -- 返回结果集为空
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "best"; ---关键词为小写英文
    +------+-----------------------+
    | id1  | value                 |
    +------+-----------------------+
    |    3 | StarRocks is the BEST | -- 可以定位到满足条件的数据行
    +------+-----------------------+
    1 row in set (0.01 sec)
    ```

- 查询条件中谓词 `MATCH` 必须作为下推谓词，所以必须在 WHERE 条件中，并且作用于索引列。

    以下表和测试数据为例：

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

    以下查询语句均不满足本要求：

  - 由于查询语句的谓词 `MATCH` 不在 WHERE 子句中，无法下推，因此查询报错。

      ```SQL
      MySQL [test]> SELECT value MATCH "test" FROM t_match;
      ERROR 1064 (HY000): Match can only used as a pushdown predicate on column with GIN in a single query.
      ```

  - 由于查询语句的谓词 `MATCH` 作用的列 `value_test` 不是索引列，因此查询报错。

      ```SQL
      MySQL [test]> SELECT * FROM t_match WHERE value_test match "test";
      ERROR 1064 (HY000): Match can only used as a pushdown predicate on column with GIN in a single query.
      ```

#### 索引列不进行分词时支持的查询

如果全文倒排索引没有对索引列进行分词处理，即 `'parser' = 'none'`。此时查询条件中如下所有下推谓词都可以使用全文倒排索引进行数据过滤：

- 表达式谓词：(NOT) LIKE、(NOT) MATCH

   :::note

   - 此时 `MATCH` 语义上等同于 `LIKE`。
   - MATCH 和 LIKE 只能支持格式为 `(NOT) <col_name> MATCH|LIKE '%keyword%'`。`keyword` 必须是字符串字面量，不支持为表达式。注意，如果 `LIKE` 不满足这种格式的话，即使查询可以正常执行，但是会退化为不使用全文倒排索引过滤数据的查询。

    :::

- 普通谓词：`==`、`!=`、`<=`、`>=`、`NOT IN`、`IN`、`IS NOT NULL`、`NOT NULL`。

## 如何判断全文倒排索引是否生效

执行查询后，您可以通过 [Query Profile](https://docs.starrocks.io/zh/docs/administration/query_profile_overview/) 的 scan 节点中的详细指标 `GinFilterRows` 和 `GinFilter`查看全文倒排索引过滤的行数以及过滤时间。
