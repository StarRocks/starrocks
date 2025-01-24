---
displayed_sidebar: docs
toc_max_heading_level: 4
sidebar_position: 50
---

# [Preview] Full-text inverted index

Since version 3.3.0, StarRocks supports full-text inverted indexes, which can break the text into smaller words, and create an index entry for each word that can show the mapping relationship between the word and its corresponding row number in the data file. For full-text searches, StarRocks queries the inverted index based on the search keywords, quickly locating the data rows that match the keywords.

The full-text inverted index is not yet supported in Pramiary Key tables and shared-data clusters.

## Overview

StarRocks stores its underlying data in the data files organized by columns. Each data file contains the full-text inverted index based on the indexed columns. The values in the indexed columns are tokenized into individual words. Each word after tokenization is treated as an index entry, mapping to the row number where the word appears. Currently supported tokenization methods for English tokenization, Chinese tokenization, multilingual tokenization, and no tokenization.

For example, if a data row contains "hello world" and its row number is 123, the full-text inverted index builds index entries based on this tokenization result and row number: hello->123, world->123.

During full-text searches, StarRocks can locate index entries containing the search keywords using full-text inverted indexes, and then quickly find the row numbers where the keywords appear, significantly reducing the number of data rows that need to be scanned.

## Basic operation

### Create full-text inverted index

Before creating a fulltext inverted index, you need to enable FE configuration item `enable_experimental_gin`.

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

Also, a fulltext inverted index can only be created in the Duplicate Key table and the table property `replicated_storage` needs to be `false`.

#### Create full-text Inverted Index at table creation

Creating a full-text inverted index on column `v` with English tokenization.

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

- The `parser` parameter specifies the tokenization method. Supported values and descriptions are as follows:
  - `none` (default): no tokenization. The entire row of data in the indexed column is treated as a single index item when the full-text inverted index is constructed.
  - `english`: English tokenization. This tokenization method typically tokenizing at any non-alphabetic character. Also, uppercase English letters are converted to lowercase. Therefore, keywords in the query conditions need to be lowercase English rather than uppercase English to leverage the full-text inverted index to locate data rows.
  - `chinese`: Chinese tokenization. This tokenization method uses the [CJK Analyzer](https://lucene.apache.org/core/6_6_1/analyzers-common/org/apache/lucene/analysis/cjk/package-summary.html) in CLucene for tokenization.
  - `standard`: Multilingual tokenization. This tokenization method provides grammar based tokenization (based on the [Unicode Text Segmentation algorithm](https://unicode.org/reports/tr29/)) and works well for most languages and cases of mixed languages, such as Chinese and English. For example, this tokenization method can distinguishes between Chinese and English when these two languages coexist. After tokenizing English, it converts uppercase English letters to lowercase. Therefore, keywords in the query conditions need to be lowercase English rather than uppercase English to leverage the full-text inverted index to locate data rows.
- The data type of the indexed column must be CHAR, VARCHAR, or STRING.

#### Add full-text inverted index after table creation

After table creation, you can add a full-text inverted index using `ALTER TABLE ADD INDEX` or `CREATE INDEX`.

```SQL
ALTER TABLE t ADD INDEX idx (v) USING GIN('parser' = 'english');
CREATE INDEX idx ON t (v) USING GIN('parser' = 'english');
```

### Manage full-text inverted index

#### View full-text inverted index

Execute `SHOW CREATE TABLE` to view full-text inverted indexes.

```SQL
MySQL [example_db]> SHOW CREATE TABLE t\G
```

#### Delete full-text inverted index

Execute `ALTER TABLE ADD INDEX` or `DROP INDEX` to delete full-text inverted indexes.

```SQL
DROP INDEX index idx on t;
ALTER TABLE t DROP index idx;
```

### Accelerate queries by full-text inverted index

After creating a full-text inverted index, you need to ensure that the system variable `enable_gin_filter` is enabled, so the inverted index can accelerate queries. Also, you need to consider whether the index column values are tokenized to determine which queries can be accelerated.

#### Supported queries when indexed column is tokenized

If the full-text inverted index tokenizes indexed columns, that is, `'parser' = 'standard|english|chinese'`, only the `MATCH` predicate is supported for data filtering using full-text inverted indexes, and the format needs to be `<col_name> (NOT) MATCH '%keyword%'`. The `keyword` must be a string literal, and does not support expressions .

1. Create a table and insert a few rows of test data.

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

2. Use the `MATCH` predicate for querying.

- Query data rows whose `value` column contains the keyword `starrocks`.

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks";
    ```

- Retrieve data rows whose `value` column contains the keyword starting with `data`.

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "data%";
    ```

**Notes:**

- During queries, keywords can be matched fuzzily using `%`, in the format of `%keyword%`. However, the keyword must contain a part of a word. For example, if the keyword is <code>starrocks&nbsp;</code>, it cannot match the word `starrocks` because it contains spaces.

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

- If English or multilingual tokenization is used to construct the full-text inverted index, uppercase English words are converted to lowercase when the full-text inverted index is actually stored. Therefore, during queries, keywords need to be lowercase instead of uppercase to utilize the full-text inverted index to locate data rows.

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
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "BEST"; -- Keyword is uppercase English
    Empty set (0.02 sec) -- Returns an empty result set
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "best"; -- Keyword is lowercase English
    +------+-----------------------+
    | id1  | value                 |
    +------+-----------------------+
    |    3 | StarRocks is the BEST | -- Can locate data rows that meet the condition
    +------+-----------------------+
    1 row in set (0.01 sec)
    ```

- The `MATCH` predicate in the query conditions must be used as a pushdown predicate, so it must be in the WHERE clause and be performed against the indexed column.

    Take the following table and test data as an example:

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

    The following query statements do not meet the requirement:

    - Because the `MATCH` predicate in the query statement is not in the WHERE clause, it can not be pushed down, resulting in a query error.

        ```SQL
        MySQL [test]> SELECT value MATCH "test" FROM t_match;
        ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
        ```

    - Because the column `value_test` against which the `MATCH` predicate in the query statement is performed is not an indexed column, the query fails.

        ```SQL
        MySQL [test]> SELECT * FROM t_match WHERE value_test match "test";
        ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
        ```

#### Supported queries when indexed column is not tokenized

If the full-text inverted index does not tokenize the indexed column, that is, `'parser' = 'none'`, all pushdown predicates in the query conditions listed below can be used for data filtering using the full-text inverted index:

- Expression predicates: (NOT) LIKE, (NOT) MATCH
  
  :::note

  - In this case, `MATCH` is semantically equivalent to `LIKE`.
  - `MATCH` and `LIKE` only support the format `(NOT) <col_name> MATCH|LIKE '%keyword%'`. The `keyword` must be a string literal and does not support expressions. Note that if `LIKE` does not meet this format, even if the query can be executed normally, it will degrade to a query that does not use the full-text inverted index to filter data.
  :::
- Regular predicates: `==`, `!=`, `<=`, `>=`, `NOT IN`, `IN`, `IS NOT NULL`, `NOT NULL`

## How to verify whether the full-text inverted index  accelerates queries

After executing the query, you can view the detailed metrics `GinFilterRows` and `GinFilter` in the scan node of the Query Profile to see the number of rows filtered and the filtering time using the full-text inverted index.
