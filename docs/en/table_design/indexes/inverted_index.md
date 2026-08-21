---
displayed_sidebar: docs
toc_max_heading_level: 4
sidebar_position: 50
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Full-text inverted index

<Beta />

Since version 3.3.0, StarRocks supports full-text inverted indexes, which can break the text into smaller words, and create an index entry for each word that can show the mapping relationship between the word and its corresponding row number in the data file. For full-text searches, StarRocks queries the inverted index based on the search keywords, quickly locating the data rows that match the keywords.

Full-text inverted indexes provide multiple implementations. The Tantivy implementation supports Duplicate Key and Primary Key tables in both shared-nothing and shared-data clusters.

## Overview

StarRocks stores its underlying data in the data files organized by columns. Each data file contains the full-text inverted index based on the indexed columns. The values in the indexed columns are tokenized into individual words. Each word after tokenization is treated as an index entry, mapping to the row number where the word appears. Currently supported tokenization methods for English tokenization, Chinese tokenization, multilingual tokenization, and no tokenization.

For example, if a data row contains "hello world" and its row number is 123, the full-text inverted index builds index entries based on this tokenization result and row number: hello->123, world->123.

During full-text searches, StarRocks can locate index entries containing the search keywords using full-text inverted indexes, and then quickly find the row numbers where the keywords appear, significantly reducing the number of data rows that need to be scanned.

## Use Tantivy inverted indexes

### Create an index

Before creating an index, enable the FE configuration `enable_experimental_gin`:

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

Set `"imp_lib" = "tantivy"` in the index properties. The following example uses the English tokenizer:

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

You can also add an index after table creation:

```sql
CREATE INDEX idx_content ON docs (content) USING GIN (
    "imp_lib" = "tantivy",
    "parser" = "english"
);
```

### Query the index

Tantivy supports the following common query forms:

```sql
-- Match any token.
SELECT * FROM docs WHERE content MATCH_ANY 'database search';

-- Match all tokens. MATCH without a wildcard also uses AND semantics.
SELECT * FROM docs WHERE content MATCH_ALL 'database search';

-- Phrase match. ~1 allows one position of slop.
SELECT * FROM docs WHERE content MATCH_PHRASE 'full search ~1';

-- Sort by BM25 relevance.
SELECT *, score()
FROM docs
WHERE content MATCH_ANY 'database search'
ORDER BY score() DESC
LIMIT 10;
```

`MATCH`, `MATCH_ANY`, `MATCH_ALL`, and `MATCH_PHRASE` must be pushdown predicates in the `WHERE` clause on the indexed column. The right operand is normally a non-empty string literal. `MATCH_ANY` and `MATCH_ALL` can also accept the result of `tokenize()`:

```sql
SELECT * FROM docs
WHERE content MATCH_ALL tokenize('english', 'Database Search');
```

The system variable `enable_gin_filter` is `true` by default. If it has been disabled, enable it again:

```sql
SET enable_gin_filter = true;
```

### Supported tokenizers

| `parser` | Description | Additional parameters |
| --- | --- | --- |
| `none` | Default. Does not tokenize text and indexes the entire value as one term. | - |
| `english` | Tokenizes English text, converts terms to lowercase, and removes English stopwords. It does not apply stemming. | - |
| `standard` | CLucene-compatible grammar-based tokenizer that recognizes terms such as email addresses and acronyms. It converts terms to lowercase and removes English stopwords. | - |
| `chinese` / `cjk` | CJK bigram tokenizer that emits overlapping adjacent character pairs. It converts ASCII text to lowercase. `cjk` is an alias for `chinese`. | - |
| `jieba` | Dictionary-based Chinese tokenization in Jieba search mode. It converts ASCII text to lowercase. | - |
| `ik` | Dictionary-based Chinese tokenization using IK. | `parser_mode` |
| `ngram` | Emits contiguous Unicode N-Grams and converts text to lowercase. | `min_gram`, `max_gram` |

`english` drops terms longer than 40 characters. `standard` limits each term to 255 characters.

Use `tokenize()` to inspect tokenizer output. The two DDL modes for `parser = 'ik'` map to `ik` and `ik_smart` in `tokenize()`. For N-Gram tokenization, use `ngram:<min_gram>:<max_gram>`:

```sql
SELECT tokenize('ik', '中华人民共和国国歌');
SELECT tokenize('ik_smart', '中华人民共和国国歌');
SELECT tokenize('ngram:2:3', 'Ab中');
```

### Index parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `imp_lib` | - | Must be `tantivy`. It must also be specified explicitly in shared-data clusters. |
| `parser` | `none` | Specifies the tokenizer. |
| `parser_mode` | `ik_max_word` | Only valid with `parser = 'ik'`. Values: `ik_max_word` for fine-grained tokenization and `ik_smart` for coarse-grained tokenization. |
| `min_gram` | - | Only valid with `parser = 'ngram'`. It must be a positive integer and specified together with `max_gram`. |
| `max_gram` | - | Only valid with `parser = 'ngram'`. It must be greater than or equal to `min_gram`. |
| `support_phrase` | `true` | Whether to store term positions. It must be `true` to use `MATCH_PHRASE`. |
| `support_bm25` | `true` | Whether to store field norms required by BM25. It must be `true` to use `score()`. |

`support_phrase` and `support_bm25` apply only to Tantivy and take effect when the index is built. Rebuild the index after changing them.

### Limitations

- Each inverted index can contain only one `CHAR`, `VARCHAR`, or `STRING` column.
- Only Duplicate Key and Primary Key tables are supported. Primary Key tables support full-row writes and row-mode partial updates, but not column-mode partial updates.
- When you add an index using `ALTER TABLE` or `CREATE INDEX` in a shared-nothing cluster, the table property `replicated_storage` must be `false`.
- Wildcard queries support `%` and `*`, but the wildcard expression is not tokenized. It directly matches indexed terms, so consider the selected tokenizer and its case-conversion rules.
- Specify phrase slop at the end of the query text in the form `'text ~N'`. A space is required before `~`, and `N` must be a non-negative integer.
- `score()` applies only to non-negated `MATCH`, `MATCH_ANY`, or `MATCH_ALL` queries. It does not apply to `MATCH_PHRASE` or wildcard queries.

## Basic operation

### Create and use a custom Tantivy text analyzer

You can define a database-scoped, immutable text analyzer and bind a GIN index to it. The analyzer definition is a
strict JSON document. StarRocks canonicalizes the document, calculates a SHA-256 digest, and stores a fixed snapshot
of the definition in the index metadata. A text analyzer cannot be replaced or modified. To change the pipeline,
create an analyzer with a new name and migrate the index explicitly.

```sql
CREATE TEXT ANALYZER product_search PROPERTIES (
  "definition" = '{
    "char_filter": [
      {"type": "unicode_normalize", "form": "nfkc"},
      {"type": "mapping", "mappings": ["C++ => cpp"]}
    ],
    "tokenizer": {"type": "jieba", "mode": "search", "hmm": true},
    "token_filter": [
      {"type": "lowercase"},
      {"type": "stop", "stopwords": ["the", "a"]},
      {"type": "length", "min": 1, "max": 40},
      {"type": "remove_punctuation"}
    ]
  }'
);

CREATE TABLE products (
  id BIGINT,
  description STRING,
  INDEX description_gin (description) USING GIN ("analyzer" = "product_search")
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replicated_storage" = "false");
```

The `analyzer` and legacy `parser` index properties are mutually exclusive. The phase-one definition supports:

- Character filters: `unicode_normalize` (`nfc`, `nfkc`, `nfd`, or `nfkd`) and literal `mapping` rules written as
  `source => target`.
- Tokenizers: `none`, `english`, `standard`, `chinese`, `cjk`, `jieba`, `ik`, and `ngram`. `jieba` supports
  `search` and `default` modes. `ik` supports `search` (`ik_smart`) and `index` (`ik_max_word`) modes. `ngram`
  requires `min_gram` and `max_gram`.
- Ordered token filters: `lowercase`, inline `stop`, `length`, and `remove_punctuation`.

External files, paths, URIs, dictionaries, and non-empty `resource_refs` are rejected. A definition is limited to
64 KiB and 16 pipeline components. A mapping filter accepts at most 256 rules, 1 KiB per rule, and 32 KiB in total.
A stop filter accepts at most 1,024 words, 256 bytes per word, and 32 KiB in total. `ngram` values must satisfy
`1 <= min_gram <= max_gram <= 32` and `max_gram - min_gram <= 16`. Runtime analysis rejects input over 1 MiB,
output over 1,000,000 tokens, and a token over 32 KiB; it never silently truncates a definition or token stream.

Use the following statements to inspect and manage analyzers:

```sql
SHOW TEXT ANALYZERS;
SHOW TEXT ANALYZERS FROM db_name;
DESC TEXT ANALYZER product_search;
SHOW CREATE TEXT ANALYZER product_search;
DROP TEXT ANALYZER product_search RESTRICT;
```

`SHOW TEXT ANALYZERS` returns one row for every named analyzer. Creating an existing name fails, and `CREATE OR
REPLACE TEXT ANALYZER` is not supported. `DROP` fails while any index references the analyzer. `SHOW CREATE TABLE`
displays only the fully qualified analyzer name; the internal definition snapshot and digest are not displayed.

To inspect exact token metadata, query the `tokenize_detail` table function. Offsets are UTF-8 byte offsets into the
original input before character filtering.

In the 3.5 privilege model, analyzer operations use database privileges: creation requires `CREATE TABLE`, dropping
requires `ALTER`, and inspection or use requires any privilege on the analyzer database.

```sql
SELECT token.*
FROM (SELECT 1) AS input,
     tokenize_detail('product_search', 'StarRocks C++ database') AS token;
-- term, position, position_length, start_offset, end_offset, token_type
```

### Create full-text inverted index

Before creating a fulltext inverted index, you need to enable FE configuration item `enable_experimental_gin`.

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

A full-text inverted index can be created on a Duplicate Key or Primary Key table. Limitations depend on the implementation. For Tantivy, see [Limitations](#limitations).

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

Execute `ALTER TABLE DROP INDEX` or `DROP INDEX` to delete full-text inverted indexes.

```SQL
DROP INDEX idx on t;
ALTER TABLE t DROP index idx;
```

### Accelerate queries by full-text inverted index

After creating a full-text inverted index, you need to ensure that the system variable `enable_gin_filter` is enabled, so the inverted index can accelerate queries. Also, you need to consider whether the index column values are tokenized to determine which queries can be accelerated.

#### Supported queries when indexed column is tokenized

When a full-text inverted index column is enabled with tokenization (`parser` = `standard` | `english` | `chinese`), only the `MATCH`, `MATCH_ANY`, or `MATCH_ALL` predicates are supported for filtering. The supported formats are:
- `<col_name> (NOT) MATCH '%keyword%'`
- `<col_name> (NOT) MATCH_ANY 'keyword1, keyword2'`
- `<col_name> (NOT) MATCH_ALL 'keyword1, keyword2'`

Here, keyword must be a string literal; expressions are not supported.
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
  
3. Use the `MATCH_ANY` predicate for querying.

- Query data rows whose `value` column contains the keyword `database` or `data`.

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH_ANY "database data";
    ```
4. Use the `MATCH_ALL` predicate for querying.

- Query data rows whose `value` column contains both the keyword `database` and `data`.

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH_ALL "database data";
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

- The `MATCH`, `MATCH_ANY`, or `MATCH_ALL` predicate in the query conditions must be used as a pushdown predicate, so it must be in the WHERE clause and be performed against the indexed column.

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

    - Because the `MATCH`, `MATCH_ANY`, or `MATCH_ALL` predicate in the query statement is not in the WHERE clause, it can not be pushed down, resulting in a query error.

        ```SQL
        MySQL [test]> SELECT value MATCH "test" FROM t_match;
        ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
        ```

    - Because the column `value_test` against which the `MATCH`, `MATCH_ANY`, or `MATCH_ALL` predicate in the query statement is performed is not an indexed column, the query fails.

        ```SQL
        MySQL [test]> SELECT * FROM t_match WHERE value_test match "test";
        ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
        ```

#### Supported queries when indexed column is not tokenized

If the full-text inverted index does not tokenize the indexed column, that is, `'parser' = 'none'`, all pushdown predicates in the query conditions listed below can be used for data filtering using the full-text inverted index:

- Expression predicates: (NOT) LIKE, (NOT) MATCH, (NOT) MATCH_ANY, (NOT) MATCH_ALL
  
  :::note

  - In this case, `MATCH` is semantically equivalent to `LIKE`.
  - `MATCH` and `LIKE` only support the format `(NOT) <col_name> MATCH|LIKE '%keyword%'`. The `keyword` must be a string literal and does not support expressions. Note that if `LIKE` does not meet this format, even if the query can be executed normally, it will degrade to a query that does not use the full-text inverted index to filter data.
  :::
- Regular predicates: `==`, `!=`, `<=`, `>=`, `NOT IN`, `IN`, `IS NOT NULL`, `NOT NULL`

## How to verify whether the full-text inverted index  accelerates queries

After executing the query, you can view the detailed metrics `GinFilterRows` and `GinFilter` in the scan node of the Query Profile to see the number of rows filtered and the filtering time using the full-text inverted index.
