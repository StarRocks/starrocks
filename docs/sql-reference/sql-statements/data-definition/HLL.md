# HLL

## description

HLL enables the development of programmes based on the HyperLogLog algorithm. It is used to store intermediate results of the HyperLogLog calculation process. It can only be used as the value column type of the table. It reduces the amount of data through aggregation so as to speed up the query process. There may be around 1% deviation in the estimated results.

HLL column is generated through imported data or data from other columns. When data is imported, hll_hash function will specify which column should be generated into hll column which is often used to replace count_distinct and calculate uv quickly with rollup.

The correlation function:

HLL_UNION_AGG(hll): This function is an aggregate function used to estimate the cardinality of all data that meet the coditions. This can also be used to analyze functions. It only supports default window and does not support window clause.

HLL_RAW_AGG(hll): This function is an aggregate function used to aggregate fields of hll type and returns with hll type.

HLL_CARDINALITY(hll): This function is used to estimate the cardinality of a single hll column.

HLL_HASH(column_name): This generates HLL column type and is used for inserts or imports. See the instructions for the use of imports.

EMPTY_HLL(): This generates empty HLL column and is used to fill in default values during inserts or imports. See the instructions for the use of imports.

## example

1. First, create a table with hll column.

    ```sql
    create table test(
    dt date,
    id int,
    name char(10),
    province char(10),
    os char(1),
    set1 hll hll_union,
    set2 hll hll_union)
    distributed by hash(id) buckets 32;
    ```

2. Import data. Please refer to "help curl" for the import method.

    ```plain text
    a. Use a table column to generate hll column 
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
    http://host/api/test_db/test/_stream_load

    b. Use a data column to generate hll column 
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
    http://host/api/test_db/test/_stream_load
    ```

3. Aggregate data in the following three ways: (Without aggregation, direct query on base table may be as slow as using approx_count_distinct)

    ```plain text
    a. Create a rollup to aggregate hll column
    alter table test add rollup test_rollup(dt, set1);

    b. Create another table to calculate uv and insert data into it

    create table test_uv(
    dt date,
    id int,
    uv_set hll hll_union)
    distributed by hash(id) buckets 32;

    insert into test_uv select dt, id, set1 from test;

    c. Create another table to calculate uv. Insert data and generate hll column by testing other columns through hll_hash

    create table test_uv(
    dt date,
    id int,
    id_set hll hll_union)
    distributed by hash(id) buckets 32;

    insert into test_uv select dt, id, hll_hash(id) from test;
    ```

4. Query. HLL column does not support direct query into its original values. It can be queried by matching functions.

    ```plain text
    a. Calculate the total nv
    select HLL_UNION_AGG(uv_set) from test_uv;

    b. Calculate uv for each day 
    select dt, HLL_CARDINALITY(uv_set) from test_uv;

    c. Calculate the aggregation value of set 1 in the test table
    select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
    select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
    ```

## keyword

HLL
