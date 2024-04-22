---
displayed_sidebar: "English"

---

# N-gram Bloom filter indexes

For more information about the principles of the Bloom filter index, and how to create and modify the Bloom filter index, see [Bloom Filter](./Bloomfilter_index.md).

The N-gram Bloom Filter index is a special Bloom Filter index only suitable for string (`STRING`, `CHAR`, or `VARCHAR`) type columns. The difference between the N-gram Bloom Filter and Bloom Filter is that the N-gram Bloom filter tokenizes the strings based on the provided `gram_num` and writes the resulting substrings into the Bloom filter.

For example, if a row in a string column contains the word `Technical`, a traditional Bloom filter would write the entire string `Technical` into the Bloom filter. In contrast, an N-gram Bloom filter with a specified `gram_num` of 4 would tokenize `Technical` into the following substrings:

```
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

Each of these six substrings is written into the Bloom filter.



## Usage notes

- You can create n-gram bloom filter indexes for all string columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create n-gram bloom filter indexes for `key` columns.
- N-gram Bloom filter indexes can be created for columns of the following data types:
  - String types: CHAR, STRING, and VARCHAR.
- To determine whether a query hits an N-gram Bloom filter index, you can check the `BloomFilterFilterRows` field in the query's profile. 
- Only one type of index (Bloom filter or N-gram Bloom filter) can be created for a single column.

## Create bloom filter indexes

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

### Index parameters

| **Parameter**    | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| `index_name`       | Yes          | The name of the index. Index names must be unique within a table. |
| `column_name`      | Yes          | The name of the column for which the index is created. Only a single column name can be specified. In the example above, it is `k2`. |
| `gram_num`         | Yes          | The length of the substring when tokenizing a string column's data for the N-gram Bloom filter. In the example above, `gram_num` is 4. |
| `bloom_filter_fpp` | No           | The false positive possibility of the Bloom filter, ranging from 0.0001 to 0.05. The default value is 0.05. A smaller value provides better filtering but incurs greater storage overhead. |
| `case_sensitive`   |  No          | Whether this index is case-sensitive or not. Default value is `case_sensitive` |
| `COMMENT`          | No           | Index comment. |

For other parameter explanations related to table creation, see [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)

## Display bloom filter indexes

You can view all indexes of a table using `SHOW CREATE TABLE` or `SHOW INDEX FROM` table. Since index creation is asynchronous, you can only see the corresponding index after the index creation is successful.

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

## Modify indexes

You can add and delete n-gram bloom filter indexes by using the [ALTER TABLE](../../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) statement.

- The following statement adds a new N-gram Bloom filter index for column `k1` with the index name `new_index_name` to the table `table1`.

  ```SQL
  ALTER TABLE table1 
  ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                              "bloom_filter_fpp" = "0.05") COMMENT '';
  ```

- The following statement removes the N-gram Bloom filter index named `new_index_name` from the table `table1`.

  ```SQL
  ALTER TABLE table1 DROP INDEX new_index_name;
  ```

:::note
Altering an index is an asynchronous operation. You can view the progress of this operation by executing [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md). You can run only one alter index task on a table each time.
:::

## Function Support
### LIKE
An n-gram bloom filter index can be used for `LIKE` queries if `gram_num` is small enough (smaller than the length of the string being queried for), otherwise, the n-gram bloom filter index will not accelerate `LIKE` queries.

For example: if `gram_num` is 4, and the query is `SELECT * FROM table WHERE col1 LIKE "%abc"`, the n-gram index is not effective for this query because "abc" only has three characters. If the query used `LIKE "%abcd"` or `LIKE "%abcde%"` etc., then the n-gram filter can be effective.

### ngram_search
When using the `ngram_search` function, if the queried column has an N-gram Bloom filter index and the `gram_num` specified in the `ngram_search` function matches the `gram_num` of the N-gram Bloom filter index, the index will automatically filter out data with a string similarity of 0, significantly speeding up the function execution process.

### ngram_search_case_insensitive
same as `ngram_search`, but when creating an n-gram bloom filter index for case-insensitive situations, you need to create the index like this:

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
or alter index if already created one:
  ```SQL
  ALTER TABLE table1 
  ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                              "bloom_filter_fpp" = "0.05",
                                              "case_sensitive" = "false") COMMENT '';
  ```