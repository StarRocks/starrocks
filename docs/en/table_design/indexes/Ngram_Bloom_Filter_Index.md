---
displayed_sidebar: "English"

---

# Ngram Bloom filter indexes

For more information about the principles of the Bloom filter index, and how to create and modify the Bloom filter index, see [Bloomfilter](./Bloomfilter_index.md).

NGRAM Bloom Filter index is a special Bloom Filter index, which is only suitable for column as a string type.The difference between NGRAM Bloom Filter and Bloom Filter is that the Ngram Bloom filter tokenizes the strings based on the provided `gram_num` and writes the resulting substrings into the Bloom filter.



For example, if a row in a string column contains the word "Technical," a traditional Bloom filter would write the entire string "Technical" into the Bloom filter. In contrast, an Ngram Bloom filter with a specified `gram_num` of 4 would tokenize "Technical" into the following substrings subsequently:

```
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

Each of these six string substrings is written into the Bloom filter.



## Usage notes

- You can create ngram bloom filter indexes for all columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create ngram bloom filter indexes for key columns.
- Ngram Bloom filter indexes can be created for columns of the following data types:
  - String types: CHAR, STRING, and VARCHAR.
- To determine whether a query hits an Ngram Bloom filter index, you can check the `BloomFilterFilterRows` field in the query's Profile. 
- Only one type of index (Bloom filter or Ngram Bloom filter) can be created for a single column.

## Create bloom filter indexes

```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.05") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("replication_num"= "1");

```

The parameters related to the index are explained as follows:

| **Parameter**    | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| index_name       | Yes          | The name of the index. The same index name cannot be created within the same table. |
| column_name      | Yes          | The name of the column for which the index is created. Only a single column name can be specified. In the example above, it is "k2". |
| gram_num         | Yes          | The length of the substring when tokenizing a string column's data for the Ngram Bloom filter. In the example above, `gram_num` is 4. |
| bloom_filter_fpp | No           | The false positive possibility of the Bloom filter, ranging from 0.0001 to 0.05. The default value is 0.05. A smaller value provides better filtering but incurs greater storage overhead. |
| COMMENT          | No           | Index comment.                                               |

For other parameter explanations related to table creation, see [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)

## Display bloom filter indexes

You can view all indexes of a table using `show create table` or `show index from table`. Since index creation is asynchronous, you can only see the corresponding index after the index creation is successful.

```SQL
SHOW CREATE TABLE table1;
show index from table1;
```

## Modify indexes

You can add and delete ngram bloom filter indexes by using the [ALTER TABLE](../../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) statement.

- The following statement adds a new Ngram Bloom filter index column `k1` with the index name `new_index_name` to the table `table1`.

  ```SQL
  ALTER TABLE table1 ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05") COMMENT '';
  ```

- The following statement removes the Ngram Bloom filter index named `new_index_name` from the table `table1`.

  ```SQL
  ALTER TABLE table1 DROP INDEX new_index_name;
  ```

> Note: Altering an index is an asynchronous operation. You can view the progress of this operation by executing [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md). You can run only one alter index task on a table each time.



# Function Support

Currently, Ngram Bloom filter indexes only support the `ngram_search` function. When using the `ngram_search` function, if the queried column has an Ngram Bloom filter index and the `gram_num` specified in the `ngram_search` function matches the `gram_num` of the Ngram Bloom filter index, the index will automatically filter out data with a string similarity of 0, significantly speeding up the function execution process.

Ngram Bloom filter indexes currently do not support the `ngram_search_case_insensitive` function.