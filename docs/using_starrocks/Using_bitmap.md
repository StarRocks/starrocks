# Use Bitmap for exact count distinct

## Background

<<<<<<< HEAD
There are usually two ways to conduct  accurate de-duplication analysis in StarRocks.
=======
There are usually two ways to conduct accurate Count Distinct in StarRocks.
>>>>>>> bf3331d96 ([Doc] update json length and modify import (#23060))

* Detail-based Count Distinct: This is a traditional count distinct approach that is able to retain detailed data for flexible analysis. However, it consumes huge computational and storage resources and is not friendly enough to support scenarios involving large-scale datasets and query latency-sensitive Count Distinct.
* Precomputation-based Count Distinct: This approach is also recommended by StarRocks. In some scenarios, users only want to get the results after Count Distinct and care less about detailed data. Such a scenario can be analyzed by precomputation, which is essentially using space for time and resonates with the core idea of the multidimensional OLAP (MOLAP) aggregation model. It is to calculate data in the process of data loading, reducing the storage cost and the cost of on-site calculation during query. You can further reduce the size of datasets for on-site computation by shrinking RollUp dimension.

## Traditional Count Distinct Calculation

StarRocks is implemented based on the MPP architecture that supports retaining detailed data when using count distinct calculation for accurate Count Distinct. However, because of the need for multiple data shuffles (transferring data across nodes and calculating de-weighting) during query, it leads to a linear decrease in performance as the data volume increases.

In the following scenario, there are tables (dt, page, user_id) that need to calculate UV by detailed data.

|  dt   |   page  | user_id |
| :---: | :---: | :---:|
|   20191206  |   game  | 101 |
|   20191206  |   shopping  | 101 |
|   20191206  |   game  | 101 |
|   20191206  |   shopping  | 101 |
|   20191206  |   game  | 101 |
|   20191206  |   shopping  | 101 |

Count `uv` grouping by `page`

|  page   |   uv  |
| :---: | :---: |
|   game  |  1   |
|   shopping  |   2  |

```sql
 select page, count(distinct user_id) as uv from table group by page;
```

For the SQL of PV calculation, StarRocks will do the calculation according to the following figure. First, group by the page column and user_id column, and then count.

![alter](../assets/6.1.2-2.png)

* Note: The figure shows a schematic of 6 rows of data computed on 2 BE nodes

Given that the data needs to be shuffled several times, it will require more computational resources and the query will be slower when the data volume gets larger. The Bitmap technology is used to  solve the performance problem of traditional count distinct calculation in such scenarios.

## Count Distinct with Bitmap

Assume there is array A with values in the range [0, n). A bitmap of byte length `floor((n+7)/8)` can be used to de-duplicate the array.

1. Initialize the bitmap to all zeros.
2. Process elements in the array one by one and use the value of the elements as the subscript of the bitmap. Set the bit of the subscript to 1.
3. Count the number of 1s in the bitmap. The number of 1s is the result of count distinct of array A.

## Advantages of bitmap Count Distinct

1. Space advantage: Using one bit of a bitmap to indicate the existence of the corresponding subscript has a great space advantage. For example, for int32 Count Distinct, the storage space required by a normal bitmap is only 1/32 of the traditional Count Distinct. The implementation of Roaring Bitmap in StarRocks further significantly reduces storage usage through optimizing sparse bitmaps.
2. Time advantage: Bitmap Count Distinct involves computation such as bit placement for a given subscript and counting the number of placed bitmaps, which are O(1) and O(n) operations respectively. The latter can be computed efficiently using clz, ctz and other instructions. In addition, bitmap Count Distinct can be accelerated in parallel in the MPP execution engine, where each computing node computes a local sub-bitmap and uses the bit_or function to merge all sub-bitmaps into a final bitmap. bit_or is more efficient than sort-based or hash-based Count Distinct in that it has no condition or data dependencies and supports vectorized execution.

For details of the implementation of Roaring Bitmap, see [specific paper and implementation](https://github.com/RoaringBitmap/RoaringBitmap).

## How to Use Bitmap

1. Both bitmap indexing and bitmap Count Distinct use the bitmap technique. However, the purpose for introducing them and the problem they solve are completely different. The former is used to filter enumerated columns with a low cardinality, while the latter is used to calculate the number of distinct elements in the value columns of a data row.
2. Currently, bitmap columns can only exist in Aggregate tables, not in Duplicate Key or Unique Key tables.
3. When creating a table, specify the data type of the value column as BITMAP and the aggregate function as `BITMAP_UNION`.
4. When using count distinct on a Bitmap column, StarRocks automatically converts count distinct to BITMAP_UNION_COUNT.

### Examples

Take the calculation of page UVs as an example.

1. Create a table with a BITMAP column `visit_users`, which uses the aggregate function `BITMAP_UNION`.

    ```sql
    CREATE TABLE `page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`) BUCKETS 1
    PROPERTIES (
      "replication_num" = "1",
      "storage_format" = "DEFAULT"
    );
    ```

2. Load data into this table.

   Use INSET INTO to load data:

    ```sql
    insert into page_uv values
    (1, '2020-06-23 01:30:30', to_bitmap(13)),
    (1, '2020-06-23 01:30:30', to_bitmap(23)),
    (1, '2020-06-23 01:30:30', to_bitmap(33)),
    (1, '2020-06-23 02:30:30', to_bitmap(13)),
    (2, '2020-06-23 01:30:30', to_bitmap(23));
    ```

    After data loading:

    * In the row `page_id = 1, visit_date = '2020-06-23 01:30:30'`, the `visit_user` field contains three bitmap elements (13, 23, 33).
    * In the row `page_id = 1, visit_date = '2020-06-23 02:30 :30'`, the `visit_user` field contains one bitmap element (13).
    * In the row `page_id = 2, visit_date = '2020-06-23 01:30:30'`, the `visit_user` field contains one bitmap element (23).

   Load data from a local file:

    ```shell
    echo -e '1,2020-06-23 01:30:30,130\n1,2020-06-23 01:30:30,230\n1,2020-06-23 01:30:30,120\n1,2020-06-23 02:30:30,133\n2,2020-06-23 01:30:30,234' > tmp.csv | 
    curl --location-trusted -u root: -H "label:label_1600960288798" \
        -H "column_separator:," \
        -H "columns:page_id,visit_date,visit_users, visit_users=to_bitmap(visit_users)" -T tmp.csv \
        http://StarRocks_be0:8040/api/db0/page_uv/_stream_load
    1,2020-06-23 01:30:30,130
    1,2020-06-23 01:30:30,230
    1,2020-06-23 01:30:30,120
    1,2020-06-23 02:30:30,133
    2,2020-06-23 01:30:30,234
    DONE
    ```

3. Calculate page UV.

    ```sql
    select page_id, count(distinct visit_users) from page_uv group by page_id;
    ```

4. Query Results.

    ```sql
    mysql> select page_id, count(distinct visit_users) from page_uv group by page_id;

    +-----------+------------------------------+
    |  page_id  | count(DISTINCT `visit_user`) |
    +-----------+------------------------------+
    |         1 |                            3 |
    |         2 |                            1 |
    +-----------+------------------------------+
    2 row in set (0.00 sec)
    ```

## Bitmap Global Dictionary

Currently, Bitmap-based Count Distinct mechanism requires the input to be integer. If the user needs to use other data types as input to the Bitmap, then the user needs to build their own global dictionary to map other types of data (such as string types) to integer types. There are several ideas for building a global dictionary.

### Hive Table-based Global Dictionary

The global dictionary itself in this scheme is a Hive table, which has two columns, one for raw values and one for encoded Int values. The steps to generate the global dictionary are as follows:

1. De-duplicate the dictionary columns of the fact table to generate a temporary table
2. Left join the temporary table and the global dictionary, add `new value` to the temporary table.
3. Encode the `new value` and insert it into the global dictionary.
4. Left join the fact table and the updated global dictionary, replace the dictionary items with IDs.

In this way, the global dictionary can be updated and the value columns in the fact table can be replaced using Spark or MR. Compared with the trie tree-based global dictionary, this approach can be distributed and the global dictionary can be reused.

However, there are a few things to note: the original fact table is read multiple times, and there are two joins that consume a lot of extra resources during the calculation of the global dictionary.

### Build a global dictionary based on a trie tree

Users can also build their own global dictionaries using trie trees (aka prefix trees or dictionary trees). The trie tree has common prefixes for the descendants of nodes, which can be used to reduce query time and minimize string comparisons, and therefore is well suited for implementing dictionary encoding. However, the implementation of trie tree is not easy to distribute and can create performance bottlenecks when the data volume is relatively large.

By building a global dictionary and converting other types of data to integer data, you can use Bitmap to perform accurate Count Distinct analysis of non-integer data columns.
