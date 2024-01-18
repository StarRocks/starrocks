---
displayed_sidebar: "English"
---

# HLL (HyperLogLog)

## Description

HLL is used for [approximate count distinct](../../../using_starrocks/Using_HLL.md).

HLL enables the development of programmes based on the HyperLogLog algorithm. It is used to store intermediate results of the HyperLogLog calculation process. It can only be used as the value column type of the table. HLL reduces the amount of data through aggregation to speed up the query process. There may be a 1% deviation in the estimated results.

HLL column is generated based on the imported data or data from other columns. When data is imported, the [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md) function specifies which column will be used to generated the HLL column. HLL is often used to replace COUNT DISTINCT and quickly calculate unique views (UVs) with rollup.

The storage space used by HLL is determined by the distinct values in the hash value. The storage space varies depending on three conditions:

- HLL is empty. No value is inserted into HLL and the storage cost is the lowest, which is 80 bytes.
- The number of distinct hash values in HLL is less than or equal to 160. The highest storage cost is 1360 bytes (80 + 160 * 8 = 1360).
- The number of distinct hash values in HLL is greater than 160. The storage cost is fixed at 16,464 bytes (80 + 16 * 1024 = 16464).

In actual business scenarios, data volume and data distribution affect the memory usage of queries and the accuracy of the approximate result. You need to consider these two factors:

- Data volume: HLL returns an approximate value. A larger data volume results in a more accurate result. A smaller data volume results in larger deviation.
- Data distribution：In the case of large data volume and high-cardinality dimension column for GROUP BY，data computation will use more memory. HLL is not recommended in this situation. It is recommended when you perform no-group-by count distinct or GROUP BY on low-cardinality dimension columns.
- Query granularity: If you query data at a large query granularity, we recommend you use the Aggregate table or materialized view to pre-aggregate data to reduce data volume.

## Related functions

- [HLL_UNION_AGG(hll)](../../sql-functions/aggregate-functions/hll_union_agg.md): This function is an aggregate function used to estimate the cardinality of all data that meet the conditions. This can also be used to analyze functions. It only supports default window and does not support window clause.

- [HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md): This function is an aggregate function used to aggregate fields of hll type and returns with hll type.

- HLL_CARDINALITY(hll): This function is used to estimate the cardinality of a single hll column.

- [HLL_HASH(column_name)](../../sql-functions/aggregate-functions/hll_hash.md): This generates HLL column type and is used for inserts or imports. See the instructions for the use of imports.

- [HLL_EMPTY](../../sql-functions/aggregate-functions/hll_empty.md): This generates empty HLL column and is used to fill in default values during inserts or imports. See the instructions for the use of imports.

## Examples

1. Create a table with HLL columns `set1` and `set2`.

    ```sql
    create table test(
    dt date,
    id int,
    name char(10),
    province char(10),
    os char(1),
    set1 hll hll_union,
    set2 hll hll_union)
    distributed by hash(id);
    ```

2. Load data by using [Stream Load](../../../loading/StreamLoad.md).

    ```plain text
    a. Use table columns to generate an HLL column.
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
    http://host/api/test_db/test/_stream_load

    b. Use data columns to generate an HLL column.
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
    http://host/api/test_db/test/_stream_load
    ```

3. Aggregate data in the following three ways: (Without aggregation, direct query on base table may be as slow as using approx_count_distinct)

    ```sql
    -- a. Create a rollup to aggregate HLL column.
    alter table test add rollup test_rollup(dt, set1);

    -- b. Create another table to calculate uv and insert data into it

    create table test_uv(
    dt date,
    id int
    uv_set hll hll_union)
    distributed by hash(id);

    insert into test_uv select dt, id, set1 from test;

    -- c. Create another table to calculate UV. Insert data and generate HLL column by testing other columns through hll_hash.

    create table test_uv(
    dt date,
    id int,
    id_set hll hll_union)
    distributed by hash(id);

    insert into test_uv select dt, id, hll_hash(id) from test;
    ```

4. Query data. HLL columns do not support direct query to its original values. It can be queried by matching functions.

    ```plain text
    a. Calculate the total UV.
    select HLL_UNION_AGG(uv_set) from test_uv;

    b. Calculate the UV for each day.
    select dt, HLL_CARDINALITY(uv_set) from test_uv;

    c. Calculate the aggregation value of set1 in the test table.
    select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
    select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
    ```
