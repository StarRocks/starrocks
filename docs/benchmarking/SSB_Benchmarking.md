# SSB Flat-table Benchmarking

Star schema benchmark (SSB) is designed to test basic performance metrics of OLAP database products. SSB uses a star schema test set that is widely applied in academia and industry. For more information, see the paper [Star Schema Benchmark](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF).
ClickHouse flattens the star schema into a wide flat table and rewrites the SSB into a single-table benchmark. For more information, see [Star schema benchmark of ClickHouse](https://clickhouse.tech/docs/en/getting-started/example-datasets/star-schema/).
This test compares the performance of StarRocks, Apache Druid, and ClickHouse against SSB single-table datasets.

## Test conclusion

- Among the 13 queries performed on SSB standard datasets, StarRocks has an overall query performance **2.1x that of ClickHouse and 8.7x that of Apache Druid**.
- After Bitmap Indexing of StarRocks is enabled, the performance is 1.3x compared to when this feature is disabled. The overall performance of StarRocks is **2.8x that of ClickHouse and 11.4x that of Apache Druid**.

![overall comparison](../assets/7.1-1.png)

## Test preparation

### Hardware

| Machine           | 3 cloud hosts                                                |
| ----------------- | ------------------------------------------------------------ |
| CPU               | 16-Core Intel (R) Xeon (R) Platinum 8269CY CPU @2.50GHz <br />Cache size: 36608 KB |
| Memory            | 64 GB                                                        |
| Network bandwidth | 5 Gbit/s                                                     |
| Disk              | ESSD                                                         |

### Software

StarRocks, Apache Druid, and ClickHouse are deployed on hosts of the same configurations.

- StarRocks: one FE and three BEs. The FE can be separately or hybrid deployed with BEs.
- ClickHouse: three nodes with distributed tables
- Apache Druid: three nodes. One is deployed with Master Servers and Data Servers, one is deployed with Query Servers and Data Servers, and the third is deployed only with Data Servers.

Kernel version: Linux 3.10.0-1160.59.1.el7.x86_64

OS version: CentOS Linux release 7.9.2009

Software version: StarRocks Community Version 3.0, ClickHouse 23.3, Apache Druid 25.0.0

## Test data and results

### Test data

| Table          | Record       | Description              |
| -------------- | ------------ | ------------------------ |
| lineorder      | 600 million  | Lineorder fact table     |
| customer       | 3 million    | Customer dimension table |
| part           | 1.4 million  | Parts dimension table     |
| supplier       | 200 thousand | Supplier dimension table |
| dates          | 2,556        | Date dimension table     |
| lineorder_flat | 600 million  | lineorder flat table     |

### Test results

The following table shows the performance test results on thirteen queries. The unit of query latency is ms. `ClickHouse vs StarRocks` in the table header means using the query response time of ClickHouse to divide the query response time of StarRocks. A larger value indicates better performance of StarRocks.

|      | StarRocks-3.0 | StarRocks-3.0-index | ClickHouse-23.3 | ClickHouse vs StarRocks | Druid-25.0.0 | Druid vs StarRocks |
| ---- | ------------- | ------------------- | ------------------- | ----------------------- | ---------------- | ------------------ |
| Q1.1 | 33            | 30                  | 48                  | 1.45                    | 430              | 13.03              |
| Q1.2 | 10            | 10                  | 15                  | 1.50                    | 270              | 27.00              |
| Q1.3 | 23            | 30                  | 14                  | 0.61                    | 820              | 35.65              |
| Q2.1 | 186           | 116                 | 301                 | 1.62                    | 760              | 4.09               |
| Q2.2 | 156           | 50                  | 273                 | 1.75                    | 920              | 5.90               |
| Q2.3 | 73            | 36                  | 255                 | 3.49                    | 910              | 12.47              |
| Q3.1 | 173           | 233                 | 398                 | 2.30                    | 1080             | 6.24               |
| Q3.2 | 120           | 80                  | 319                 | 2.66                    | 850              | 7.08               |
| Q3.3 | 123           | 30                  | 227                 | 1.85                    | 890              | 7.24               |
| Q3.4 | 13            | 16                  | 18                  | 1.38                    | 750              | 57.69              |
| Q4.1 | 203           | 196                 | 469                 | 2.31                    | 1230             | 6.06               |
| Q4.2 | 73            | 76                  | 160                 | 2.19                    | 1020             | 13.97              |
| Q4.3 | 50            | 36                  | 148                 | 2.96                    | 820              | 16.40              |
| sum  | 1236          | 939                 | 2645                | 2.14                    | 10750            | 8.70               |

## Test procedure

For more information about how to create a ClickHouse table and load data to the table, see [ClickHouse official doc](https://clickhouse.tech/docs/en/getting-started/example-datasets/star-schema/). The following sections describe data generation and data loading of StarRocks.

### Generate data

Download the ssb-poc toolkit and compile it.

```Bash
wget https://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/ssb-poc-1.0.zip
unzip ssb-poc-1.0.zip
cd ssb-poc-1.0/
make && make install
cd output/
```

After the compilation, all the related tools are installed to the `output` directories and the following operations are all performed under this directory.

First, generate data for SSB standard dataset `scale factor=100`.

```Bash
sh bin/gen-ssb.sh 100 data_dir
```

### Create table schema

1. Modify the configuration file `conf/starrocks.conf` and specify the cluster address. Pay special attention to `mysql_host` and `mysql_port`.

2. Run the following command to create a table:

    ```SQL
    sh bin/create_db_table.sh ddl_100
    ```

### Query data

```Bash
sh bin/benchmark.sh ssb-flat
```

### Enable Bitmap Indexing

StarRocks performs better with Bitmap Indexing enabled. If you want to test the performance of StarRocks with Bitmap Indexing enabled, especially on Q2.2, Q2.3, and Q3.3, you can create Bitmap Indexes for all STRING columns.

1. Create another `lineorder_flat` table and create Bitmap Indexes.

    ```SQL
    sh bin/create_db_table.sh ddl_100_bitmap_index
    ```

2. Add the following configuration to the `be.conf` file of all BEs and restart the BEs for the configurations to take effect.

    ```SQL
    bitmap_max_filter_ratio=1000
    ```

3. Run the data loading script.

    ```SQL
    sh bin/flat_insert.sh data_dir
    ```

After data is loaded, wait for data version compaction to complete and then perform [4.4](#query-data) again to query the data after Bitmap Indexing is enabled.

You can view the progress of data version compaction by running `select CANDIDATES_NUM from information_schema.be_compactions`. For the three BE nodes, the following results show compaction is completed:

```SQL
mysql> select CANDIDATES_NUM from information_schema.be_compactions;
+----------------+
| CANDIDATES_NUM |
+----------------+
|              0 |
|              0 |
|              0 |
+----------------+
3 rows in set (0.01 sec)
```

## Test SQL and table creation statements

### Test SQL

```SQL
--Q1.1 
SELECT sum(lo_extendedprice * lo_discount) AS `revenue` 
FROM lineorder_flat 
WHERE lo_orderdate >= '1993-01-01' and lo_orderdate <= '1993-12-31'
AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25; 
 
--Q1.2 
SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder_flat  
WHERE lo_orderdate >= '1994-01-01' and lo_orderdate <= '1994-01-31'
AND lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35; 
 
--Q1.3 
SELECT sum(lo_extendedprice * lo_discount) AS revenue 
FROM lineorder_flat 
WHERE weekofyear(lo_orderdate) = 6
AND lo_orderdate >= '1994-01-01' and lo_orderdate <= '1994-12-31' 
AND lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 26 AND 35; 
 
--Q2.1 
SELECT sum(lo_revenue), year(lo_orderdate) AS year,  p_brand 
FROM lineorder_flat 
WHERE p_category = 'MFGR#12' AND s_region = 'AMERICA' 
GROUP BY year, p_brand 
ORDER BY year, p_brand; 
 
--Q2.2
SELECT 
sum(lo_revenue), year(lo_orderdate) AS year, p_brand 
FROM lineorder_flat 
WHERE p_brand >= 'MFGR#2221' AND p_brand <= 'MFGR#2228' AND s_region = 'ASIA' 
GROUP BY year, p_brand 
ORDER BY year, p_brand; 
  
--Q2.3
SELECT sum(lo_revenue), year(lo_orderdate) AS year, p_brand 
FROM lineorder_flat 
WHERE p_brand = 'MFGR#2239' AND s_region = 'EUROPE' 
GROUP BY year, p_brand 
ORDER BY year, p_brand; 
 
--Q3.1
SELECT
    c_nation,
    s_nation,
    year(lo_orderdate) AS year,
    sum(lo_revenue) AS revenue FROM lineorder_flat 
WHERE c_region = 'ASIA' AND s_region = 'ASIA' AND lo_orderdate >= '1992-01-01'
AND lo_orderdate <= '1997-12-31' 
GROUP BY c_nation, s_nation, year 
ORDER BY  year ASC, revenue DESC; 
 
--Q3.2 
SELECT c_city, s_city, year(lo_orderdate) AS year, sum(lo_revenue) AS revenue
FROM lineorder_flat 
WHERE c_nation = 'UNITED STATES' AND s_nation = 'UNITED STATES'
AND lo_orderdate  >= '1992-01-01' AND lo_orderdate <= '1997-12-31' 
GROUP BY c_city, s_city, year 
ORDER BY year ASC, revenue DESC; 
 
--Q3.3 
SELECT c_city, s_city, year(lo_orderdate) AS year, sum(lo_revenue) AS revenue 
FROM lineorder_flat 
WHERE c_city in ( 'UNITED KI1' ,'UNITED KI5') AND s_city in ('UNITED KI1', 'UNITED KI5')
AND lo_orderdate  >= '1992-01-01' AND lo_orderdate <= '1997-12-31' 
GROUP BY c_city, s_city, year 
ORDER BY year ASC, revenue DESC; 
 
--Q3.4 
SELECT c_city, s_city, year(lo_orderdate) AS year, sum(lo_revenue) AS revenue 
FROM lineorder_flat 
WHERE c_city in ('UNITED KI1', 'UNITED KI5') AND s_city in ('UNITED KI1', 'UNITED KI5')
AND lo_orderdate  >= '1997-12-01' AND lo_orderdate <= '1997-12-31' 
GROUP BY c_city, s_city, year 
ORDER BY year ASC, revenue DESC; 
 
--Q4.1 
SELECT year(lo_orderdate) AS year, c_nation, sum(lo_revenue - lo_supplycost) AS profit
FROM lineorder_flat 
WHERE c_region = 'AMERICA' AND s_region = 'AMERICA' AND p_mfgr in ('MFGR#1', 'MFGR#2') 
GROUP BY year, c_nation 
ORDER BY year ASC, c_nation ASC; 
 
--Q4.2 
SELECT year(lo_orderdate) AS year, 
    s_nation, p_category, sum(lo_revenue - lo_supplycost) AS profit 
FROM lineorder_flat 
WHERE c_region = 'AMERICA' AND s_region = 'AMERICA'
AND lo_orderdate >= '1997-01-01' and lo_orderdate <= '1998-12-31'
AND p_mfgr in ( 'MFGR#1' , 'MFGR#2') 
GROUP BY year, s_nation, p_category 
ORDER BY year ASC, s_nation ASC, p_category ASC; 
 
--Q4.3 
SELECT year(lo_orderdate) AS year, s_city, p_brand, 
    sum(lo_revenue - lo_supplycost) AS profit 
FROM lineorder_flat 
WHERE s_nation = 'UNITED STATES'
AND lo_orderdate >= '1997-01-01' and lo_orderdate <= '1998-12-31'
AND p_category = 'MFGR#14' 
GROUP BY year, s_city, p_brand 
ORDER BY year ASC, s_city ASC, p_brand ASC; 
```

### Table creation statements

#### Default `lineorder_flat` table

The following statement matches the current cluster size and data size (three BEs, scale factor = 100). If your cluster has more BE nodes or larger data size, you can adjust the number of buckets, create the table again, and load data again to achieve better test results.

<<<<<<< HEAD
### 4.1 Generate Data

1. Download the ssb-poc toolkit and compile it.

    ```Bash
    wget https://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/ssb-poc-0.10.0.zip
    unzip ssb-poc-0.10.0.zip
    cd ssb-poc-0.10.0
    cd ssb-poc
    make && make install  
    ```

2. Install all the related tools to the output directory.
3. Go to the output directory and generate data.

    ```Bash
    cd output
    bin/gen-ssb.sh 100 data_dir
    ```

### 4.2 Create Table Schema

1. Modify the configuration file conf/starrocks.conf and specify the cluster address.

    ```Bash
    # for mysql cmd
    mysql_host: 192.168.1.1
    mysql_port: 9030
    mysql_user: root
    mysql_password:
    database: ssb

    # cluster ports
      http_port: 8030
      be_heartbeat_port: 9050
      broker_port: 8000

    # parallel_fragment_exec_instance_num  Set the parallelism. The recommended value is half the logical cores of each cluster node. Use 8 for example.
      parallel_num: 8
    ...
    ```

2. Run the following script to create a table:

    ```SQL
    # Test 100 GB data.
    bin/create_db_table.sh ddl_100
    ```

   Following is the statement for creating table lineorder_flat. The default bucket number has been specified. You can delete the table and re-plan the bucket number based on your cluster size and node configuration, which helps achieve better test results.

    ```SQL
    CREATE TABLE `lineorder_flat` (
      `lo_orderdate` date NOT NULL COMMENT "",
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_linenumber` tinyint(4) NOT NULL COMMENT "",
      `lo_custkey` int(11) NOT NULL COMMENT "",
      `lo_partkey` int(11) NOT NULL COMMENT "",
      `lo_suppkey` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(100) NOT NULL COMMENT "",
      `lo_shippriority` tinyint(4) NOT NULL COMMENT "",
      `lo_quantity` tinyint(4) NOT NULL COMMENT "",
      `lo_extendedprice` int(11) NOT NULL COMMENT "",
      `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
      `lo_discount` tinyint(4) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
      `lo_supplycost` int(11) NOT NULL COMMENT "",
      `lo_tax` tinyint(4) NOT NULL COMMENT "",
      `lo_commitdate` date NOT NULL COMMENT "",
      `lo_shipmode` varchar(100) NOT NULL COMMENT "",
      `c_name` varchar(100) NOT NULL COMMENT "",
      `c_address` varchar(100) NOT NULL COMMENT "",
      `c_city` varchar(100) NOT NULL COMMENT "",
      `c_nation` varchar(100) NOT NULL COMMENT "",
      `c_region` varchar(100) NOT NULL COMMENT "",
      `c_phone` varchar(100) NOT NULL COMMENT "",
      `c_mktsegment` varchar(100) NOT NULL COMMENT "",
      `s_region` varchar(100) NOT NULL COMMENT "",
      `s_nation` varchar(100) NOT NULL COMMENT "",
      `s_city` varchar(100) NOT NULL COMMENT "",
      `s_name` varchar(100) NOT NULL COMMENT "",
      `s_address` varchar(100) NOT NULL COMMENT "",
      `s_phone` varchar(100) NOT NULL COMMENT "",
      `p_name` varchar(100) NOT NULL COMMENT "",
      `p_mfgr` varchar(100) NOT NULL COMMENT "",
      `p_category` varchar(100) NOT NULL COMMENT "",
      `p_brand` varchar(100) NOT NULL COMMENT "",
      `p_color` varchar(100) NOT NULL COMMENT "",
      `p_type` varchar(100) NOT NULL COMMENT "",
      `p_size` tinyint(4) NOT NULL COMMENT "",
      `p_container` varchar(100) NOT NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`lo_orderdate`, `lo_orderkey`)
    COMMENT "OLAP"
    PARTITION BY RANGE(`lo_orderdate`)
    (START ("1992-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR))
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
    PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
    );
    ```

3. Modify page_cache parameters and restart the BE node.

    ```SQL
      disable_storage_page_cache=false; -- Enable page_cache.
      storage_page_cache_limit=4294967296; -- Specify page_cache_limit.
    ```

   If you want to test the performance of StarRocks with bitmap indexing enabled, you can perform the following steps. If you want to test standard performance, skip this step and proceed with data loading.

4. Create bitmap indexes for all string columns.

    ```SQL
    #Create bitmap indexes for lo_orderpriority, lo_shipmode, c_name, c_address, c_city, c_nation, c_region, c_phone, c_mktsegment, s_region, s_nation, s_city, s_name, s_address, s_phone, p_name, p_mfgr, p_category, p_brand, p_color, p_type, p_container.
    CREATE INDEX bitmap_lo_orderpriority ON lineorder_flat (lo_orderpriority) USING BITMAP;
    CREATE INDEX bitmap_lo_shipmode ON lineorder_flat (lo_shipmode) USING BITMAP;
    CREATE INDEX bitmap_c_name ON lineorder_flat (c_name) USING BITMAP;
    CREATE INDEX bitmap_c_address ON lineorder_flat (c_address) USING BITMAP;
    CREATE INDEX bitmap_c_city ON lineorder_flat (c_city) USING BITMAP;
    CREATE INDEX bitmap_c_nation ON lineorder_flat (c_nation) USING BITMAP;
    CREATE INDEX bitmap_c_region ON lineorder_flat (c_region) USING BITMAP;
    CREATE INDEX bitmap_c_phone ON lineorder_flat (c_phone) USING BITMAP;
    CREATE INDEX bitmap_c_mktsegment ON lineorder_flat (c_mktsegment) USING BITMAP;
    CREATE INDEX bitmap_s_region ON lineorder_flat (s_region) USING BITMAP;
    CREATE INDEX bitmap_s_nation ON lineorder_flat (s_nation) USING BITMAP;
    CREATE INDEX bitmap_s_city ON lineorder_flat (s_city) USING BITMAP;
    CREATE INDEX bitmap_s_name ON lineorder_flat (s_name) USING BITMAP;
    CREATE INDEX bitmap_s_address ON lineorder_flat (s_address) USING BITMAP;
    CREATE INDEX bitmap_s_phone ON lineorder_flat (s_phone) USING BITMAP;
    CREATE INDEX bitmap_p_name ON lineorder_flat (p_name) USING BITMAP;
    CREATE INDEX bitmap_p_mfgr ON lineorder_flat (p_mfgr) USING BITMAP;
    CREATE INDEX bitmap_p_category ON lineorder_flat (p_category) USING BITMAP;
    CREATE INDEX bitmap_p_brand ON lineorder_flat (p_brand) USING BITMAP;
    CREATE INDEX bitmap_p_color ON lineorder_flat (p_color) USING BITMAP;
    CREATE INDEX bitmap_p_type ON lineorder_flat (p_type) USING BITMAP;
    CREATE INDEX bitmap_p_container ON lineorder_flat (p_container) USING BITMAP;
    ```

5. Modify the following BE parameter and restart the BE node.

    ```SQL
    bitmap_max_filter_ratio=1000; 
    ```

### 4.3 Import Data

1. Use Stream Load to import single-table data.

    ```Bash
    bin/stream_load.sh data_dir
    ```

2. Insert data into the flat table lineorder_flat.

    ```Bash
    bin/flat_insert.sh 
    ```

### 4.4 Query Data

1. SSB query

    ```Bash
    bin/benchmark.sh -p -d ssb
    bin/benchmark.sh -p -d ssb-flat
    ```
=======
```SQL
CREATE TABLE `lineorder_flat` (
  `LO_ORDERDATE` date NOT NULL COMMENT "",
  `LO_ORDERKEY` int(11) NOT NULL COMMENT "",
  `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT "",
  `LO_CUSTKEY` int(11) NOT NULL COMMENT "",
  `LO_PARTKEY` int(11) NOT NULL COMMENT "",
  `LO_SUPPKEY` int(11) NOT NULL COMMENT "",
  `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT "",
  `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT "",
  `LO_QUANTITY` tinyint(4) NOT NULL COMMENT "",
  `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT "",
  `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT "",
  `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT "",
  `LO_REVENUE` int(11) NOT NULL COMMENT "",
  `LO_SUPPLYCOST` int(11) NOT NULL COMMENT "",
  `LO_TAX` tinyint(4) NOT NULL COMMENT "",
  `LO_COMMITDATE` date NOT NULL COMMENT "",
  `LO_SHIPMODE` varchar(100) NOT NULL COMMENT "",
  `C_NAME` varchar(100) NOT NULL COMMENT "",
  `C_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `C_CITY` varchar(100) NOT NULL COMMENT "",
  `C_NATION` varchar(100) NOT NULL COMMENT "",
  `C_REGION` varchar(100) NOT NULL COMMENT "",
  `C_PHONE` varchar(100) NOT NULL COMMENT "",
  `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT "",
  `S_NAME` varchar(100) NOT NULL COMMENT "",
  `S_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `S_CITY` varchar(100) NOT NULL COMMENT "",
  `S_NATION` varchar(100) NOT NULL COMMENT "",
  `S_REGION` varchar(100) NOT NULL COMMENT "",
  `S_PHONE` varchar(100) NOT NULL COMMENT "",
  `P_NAME` varchar(100) NOT NULL COMMENT "",
  `P_MFGR` varchar(100) NOT NULL COMMENT "",
  `P_CATEGORY` varchar(100) NOT NULL COMMENT "",
  `P_BRAND` varchar(100) NOT NULL COMMENT "",
  `P_COLOR` varchar(100) NOT NULL COMMENT "",
  `P_TYPE` varchar(100) NOT NULL COMMENT "",
  `P_SIZE` tinyint(4) NOT NULL COMMENT "",
  `P_CONTAINER` varchar(100) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`LO_ORDERDATE`, `LO_ORDERKEY`)
COMMENT "OLAP"
PARTITION BY date_trunc('year', `LO_ORDERDATE`)
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 48
PROPERTIES ("replication_num" = "1");
```
>>>>>>> branch-2.5

#### `lineorder_flat` table with Bitmap Indexes

```SQL
CREATE TABLE `lineorder_flat` (
  `LO_ORDERDATE` date NOT NULL COMMENT "",
  `LO_ORDERKEY` int(11) NOT NULL COMMENT "",
  `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT "",
  `LO_CUSTKEY` int(11) NOT NULL COMMENT "",
  `LO_PARTKEY` int(11) NOT NULL COMMENT "",
  `LO_SUPPKEY` int(11) NOT NULL COMMENT "",
  `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT "",
  `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT "",
  `LO_QUANTITY` tinyint(4) NOT NULL COMMENT "",
  `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT "",
  `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT "",
  `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT "",
  `LO_REVENUE` int(11) NOT NULL COMMENT "",
  `LO_SUPPLYCOST` int(11) NOT NULL COMMENT "",
  `LO_TAX` tinyint(4) NOT NULL COMMENT "",
  `LO_COMMITDATE` date NOT NULL COMMENT "",
  `LO_SHIPMODE` varchar(100) NOT NULL COMMENT "",
  `C_NAME` varchar(100) NOT NULL COMMENT "",
  `C_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `C_CITY` varchar(100) NOT NULL COMMENT "",
  `C_NATION` varchar(100) NOT NULL COMMENT "",
  `C_REGION` varchar(100) NOT NULL COMMENT "",
  `C_PHONE` varchar(100) NOT NULL COMMENT "",
  `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT "",
  `S_NAME` varchar(100) NOT NULL COMMENT "",
  `S_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `S_CITY` varchar(100) NOT NULL COMMENT "",
  `S_NATION` varchar(100) NOT NULL COMMENT "",
  `S_REGION` varchar(100) NOT NULL COMMENT "",
  `S_PHONE` varchar(100) NOT NULL COMMENT "",
  `P_NAME` varchar(100) NOT NULL COMMENT "",
  `P_MFGR` varchar(100) NOT NULL COMMENT "",
  `P_CATEGORY` varchar(100) NOT NULL COMMENT "",
  `P_BRAND` varchar(100) NOT NULL COMMENT "",
  `P_COLOR` varchar(100) NOT NULL COMMENT "",
  `P_TYPE` varchar(100) NOT NULL COMMENT "",
  `P_SIZE` tinyint(4) NOT NULL COMMENT "",
  `P_CONTAINER` varchar(100) NOT NULL COMMENT "",
  index bitmap_lo_orderpriority (lo_orderpriority) USING BITMAP,
  index bitmap_lo_shipmode (lo_shipmode) USING BITMAP,
  index bitmap_c_name (c_name) USING BITMAP,
  index bitmap_c_address (c_address) USING BITMAP,
  index bitmap_c_city (c_city) USING BITMAP,
  index bitmap_c_nation (c_nation) USING BITMAP,
  index bitmap_c_region (c_region) USING BITMAP,
  index bitmap_c_phone (c_phone) USING BITMAP,
  index bitmap_c_mktsegment (c_mktsegment) USING BITMAP,
  index bitmap_s_region (s_region) USING BITMAP,
  index bitmap_s_nation (s_nation) USING BITMAP,
  index bitmap_s_city (s_city) USING BITMAP,
  index bitmap_s_name (s_name) USING BITMAP,
  index bitmap_s_address (s_address) USING BITMAP,
  index bitmap_s_phone (s_phone) USING BITMAP,
  index bitmap_p_name (p_name) USING BITMAP,
  index bitmap_p_mfgr (p_mfgr) USING BITMAP,
  index bitmap_p_category (p_category) USING BITMAP,
  index bitmap_p_brand (p_brand) USING BITMAP,
  index bitmap_p_color (p_color) USING BITMAP,
  index bitmap_p_type (p_type) USING BITMAP,
  index bitmap_p_container (p_container) USING BITMAP
) ENGINE=OLAP
DUPLICATE KEY(`LO_ORDERDATE`, `LO_ORDERKEY`)
COMMENT "OLAP"
PARTITION BY date_trunc('year', `LO_ORDERDATE`)
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 48
PROPERTIES ("replication_num" = "1");
```
