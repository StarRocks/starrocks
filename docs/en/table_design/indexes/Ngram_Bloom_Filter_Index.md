---
displayed_sidebar: "English"

---

# N-Gram Bloom Filter index

The N-Gram Bloom Filter index is a special [Bloom Filter index](./Bloomfilter_index.md) only suitable for string (`STRING`, `CHAR`, or `VARCHAR`) type columns. The difference between the N-Gram Bloom Filter and Bloom Filter is that the N-Gram Bloom Filter tokenizes the strings and writes the resulting substrings into the Bloom Filter.

For example, if a row in a string column contains the word `Technical`, a traditional Bloom Filter would write the entire string `Technical` into the Bloom Filter. In contrast, an N-Gram Bloom Filter with a specified `gram_num` of 4 would tokenize `Technical` into the following substrings:

```sql
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

Each of these six substrings is written into the Bloom Filter.

## Usage notes

- You can create N-Gram Bloom Filter indexes for all string columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create N-Gram Bloom Filter indexes for key columns.
- N-Gram Bloom Filter indexes can be created for columns of the string types (CHAR, STRING, and VARCHAR).
- To determine whether a query hits an N-Gram Bloom Filter index, you can check the `BloomFilterFilterRows` field in the query's profile.
- Only one type of index (Bloom Filter or N-gram Bloom Filter) can be created for a single column.

## Basic operation 

### Create N-Gram Bloom Filter index

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

N-Gram Bloom Filter index related parameters:

| **Parameter**    | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| `index_name`       | Yes          | The name of the index. Index names must be unique within a table. |
| `column_name`      | Yes          | The name of the column for which the index is created. Only a single column name can be specified. In the example above, it is `k2`. |
| `gram_num`         | Yes          | The length of the substring when tokenizing a string column's data for the N-Gram Bloom Filter. In the example above, `gram_num` is 4. |
| `bloom_filter_fpp` | No           | The false positive possibility of the Bloom Filter, ranging from 0.0001 to 0.05. The default value is 0.05. A smaller value provides better filtering but incurs greater storage overhead. |
| `case_sensitive`   |  No          | Whether this index is case-sensitive or not. Default value is `case_sensitive`. |
| `COMMENT`          | No           | Index comment. |

For explanations of other parameter related to table creation, see [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md).

### View N-Gram Bloom Filter index 

You can view all indexes of a table using `SHOW CREATE TABLE` or `SHOW INDEX FROM` table. Since index creation is asynchronous, you can only see the corresponding index after the index creation is successful.

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

### Modify N-Gram Bloom Filter index

You can add and delete N-Gram Bloom Filter indexes by using the [ALTER TABLE](../../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) statement.

- The following statement adds a new N-Gram Bloom Filter index for column `k1` with the index name `new_index_name` to the table `table1`.

  ```SQL
  ALTER TABLE table1 
  ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                              "bloom_filter_fpp" = "0.05") COMMENT '';
  ```

- The following statement removes the N-Gram Bloom Filter index named `new_index_name` from the table `table1`.

  ```SQL
  ALTER TABLE table1 DROP INDEX new_index_name;
  ```

:::note

Altering an index is an asynchronous operation. You can view the progress of this operation by executing [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md). You can run only one alter index task on a table each time.

:::

## Query that can be accelerated

### `LIKE` queries 

An N-Gram Bloom Filter index can be used for `LIKE` queries if `gram_num` is small enough (smaller than the length of the string being queried for). Otherwise, the N-Gram Bloom Filter index will not accelerate `LIKE` queries.

For example, if `gram_num` is 4, and the query is `SELECT * FROM table WHERE col1 LIKE "%abc"`, the N-Gram Bloom Filter is not effective for this query because `abc` only has three characters. If the query uses `LIKE "%abcd"` or `LIKE "%abcde%"` etc., then the N-Gram Bloom Filter can be effective.

### ngram_search

When using the `ngram_search` function, if the queried column has an N-Gram Bloom Filter index and the `gram_num` specified in the `ngram_search` function matches the `gram_num` of the N-Gram Bloom Filter index, the index will automatically filter out data with a string similarity of 0, significantly speeding up the function execution process.

### ngram_search_case_insensitive

The usage of this function is the same as `ngram_search`, except that this function is case-sensitive. So it requires `case_sensitive` to be `false` when the N-Gram Bloom Filter index is created.

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

If the index is already created and its paramter `case_sensitive` is set to be `true`, you can alter the paramter of this index by using:

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                            "bloom_filter_fpp" = "0.05",
                                            "case_sensitive" = "false") COMMENT '';
```
