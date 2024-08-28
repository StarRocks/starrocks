---
displayed_sidebar: docs

---

# [Preview] N-gram bloom filter index

The N-gram bloom filter index is a special [Bloom filter index](./Bloomfilter_index.md) which is typically used to accelerate the LIKE queries and the calculation speed of the `ngram_search` and `ngram_search_case_insensitive` functions.

The N-gram bloom filter index is only suitable for string (`STRING`, `CHAR`, or `VARCHAR`) type columns. The difference between the N-gram bloom filter index and Bloom filter index is that the N-gram bloom filter index first tokenizes the strings and then writes the resulting substrings into the Bloom filter index. For example, a certain indexed column value is a string `Technical`. For a traditional Bloom filter index, the entire string `Technical` would be directly written into the index. However, for an N-gram bloom filter index with a specified `gram_num` of `4`, the string `Technical` would be first tokenized into the following substrings:

```sql
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

And then each of these six substrings is written into the Bloom filter index.

## Usage notes

- For a Duplicate Key or Primary Key table, you can create N-gram bloom filter indexes for all the columns (of the string types). For an Aggregate table or Unique Key table, you can only create N-gram bloom filter indexes on the key columns (of the string types).
- N-gram bloom filter indexes can only be created for columns of the string types (CHAR, STRING, and VARCHAR).
- To determine whether a query hits an N-gram bloom filter index, you can check the `BloomFilterFilterRows` field in the query's profile.
- Only one type of index (Bloom Filter or N-gram Bloom Filter) can be created for a single column.

## Basic operation 

### Create N-gram bloom filter index

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

N-gram bloom filter index-related parameters:

| **Parameter**    | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| `index_name`       | Yes          | The name of the index. Index names must be unique within a table. |
| `column_name`      | Yes          | The name of the column for which the index is created. Only a single column name can be specified. In the example above, it is `k2`. |
| `gram_num`         | NO          | The length of a substring after the string in the indexed column is tokenized.The default value is 2. |
| `bloom_filter_fpp` | No           | The false positive possibility of the Bloom filter, ranging from 0.0001 to 0.05. The default value is 0.05. A smaller value provides better filtering but incurs greater storage overhead. |
| `case_sensitive`   |  No          | Whether this index is case-sensitive or not. Default value is `true`. |
| `COMMENT`          | No           | Index comment. |

For explanations of other parameters related to table creation, see [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).

### View N-gram bloom filter index 

You can view all indexes of a table using `SHOW CREATE TABLE` or `SHOW INDEX FROM` table. Since index creation is asynchronous, you can only see the corresponding index after the index is successfully created.

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

### Modify N-gram bloom filter index

You can add and delete the N-gram bloom filter index by using the [ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) statement.

- Execute the following statement to add a new N-gram bloom filter index `new_index_name` for the column `k1` to the table `table1`.

  ```SQL
  ALTER TABLE table1 
  ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                              "bloom_filter_fpp" = "0.05") COMMENT '';
  ```

-  Execute the following statement to remove the N-gram bloom filter index `new_index_name` from the table `table1`.

  ```SQL
  ALTER TABLE table1 DROP INDEX new_index_name;
  ```

:::note

Altering an index is an asynchronous operation. You can view the progress of this operation by executing [SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md). You can run only one task to alter the index in a table each time.

:::

## Query that can be accelerated

### `LIKE` queries 

An N-gram bloom filter index can accelerate `LIKE` queries if `gram_num` is small enough (smaller than the length of the string being queried for). Otherwise, the N-gram bloom filter index can not accelerate `LIKE` queries.

For example, if `gram_num` is `4`, and the query statement is `SELECT * FROM table WHERE col1 LIKE "%abc"`, the N-Gram Bloom filter index will not accelerate this query, because the queried string is `abc`, which has only three characters, less than the value `4` of `gram_num`. If the query condition is `WHERE col1 LIKE "%abcd"` or `WHERE col1 LIKE "%abcde%"`, the N-Gram Bloom filter index will accelerate the query.

### ngram_search

When the `ngram_search` function is used in the query, if the column specified in the function has an N-gram bloom filter index and the `gram_num` specified in the function matches the `gram_num` of the N-gram bloom filter index, the index will automatically filter out data with a string similarity of 0, significantly speeding up the function execution process.

### ngram_search_case_insensitive

The usage of this function is the same as `ngram_search`, except that this function is case-sensitive. So it requires `case_sensitive` to be `false` when the N-gram bloom filter index is created.

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

If the index is already created and its parameter `case_sensitive` is set to be `true`, you can alter the parameter of this index to `false` by using:

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                            "bloom_filter_fpp" = "0.05",
                                            "case_sensitive" = "false") COMMENT '';
```
