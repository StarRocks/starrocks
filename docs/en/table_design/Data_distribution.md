# Data distribution

## Technical terms

* **Data distribution**: Data distribution aims to evenly distribute data on different nodes according to certain rules, to optimize the concurrency performance of the cluster.
* **Short-scan query**: Short-scan query, refers to the query that scans a small amount of data, and can be completed by a single machine.
* **long-scan query**: Long-scan query, refers to the query that scans a large amount of data, and can be completed by multiple machines in parallel to significantly improve performance.

## Data distribution overview

The four common types of data distribution are (a) Round-Robin, (b) Range, (c) List, and (d) Hash (DeWitt and Gray, 1992). As the Exmaple below shows:

![Data distribution method](../assets/3.3.2-1.png)

* **Round-Robin** : Places the data on adjacent nodes one by one in a rotating manner.
* **Range** : Distributes the data by intervals. The intervals \[1-3\], \[4-6\] in the Exmaple correspond to different Ranges respectively.
* **List** : Distributes the data based on discrete individual values (e.g. gender, province). Each discrete value will be mapped to a node, and different fetch values may also be mapped to the same node.
* **Hash** : Maps data to different nodes by hash function.

To divide data more flexibly, modern distributed databases may “mix-and-match” the four data distribution methods for some use cases. The common combination methods are Hash-Hash, Range-Hash, and Hash-List.

## StarRocks Data Distribution

### Data Distribution Method

StarRocks uses partitioning followed by bucketing to support two types of distribution:

* Hash distribution: The entire table is treated as a partition, and the number of buckets is specified.
* Range-Hash distribution: Specify the number of partitions and the number of buckets for each partition.

~~~ SQL
-- Table creation statements using Hash distribution
CREATE TABLE site_access(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: Exmaple 3.2：Table creation statements using hash distribution

~~~ SQL
-- Table creation statement using Range-Hash distribution
CREATE TABLE site_access(
event_day DATE,
site_id INT DEFAULT '10',
city_code VARCHAR(100),
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)
(
PARTITION p1 VALUES LESS THAN ('2020-01-31'),
PARTITION p2 VALUES LESS THAN ('2020-02-29'),
PARTITION p3 VALUES LESS THAN ('2020-03-31')
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: Exmaple 3.3：Table creation statement using Range-Hash  distribution

Range distribution is known as partitioning. The column used for partitioning is called partition column. The `event_day` in Exmaple 3.3 is the partition column. Hash distribution is known as bucketing. The column used for bucketing is called bucket column. The bucket column in Exmaple 3.2 and 3.3 is `site_id`.

Range partitions can be dynamically added and deleted. In Exmaple 3.3, if new data comes in that belongs to a new month, users can add a new partition. Once determined, the number of buckets cannot be adjusted.

When using the Range-Hash distribution method, by default, the newly created partition has the same number of buckets as the original partition. Users can adjust the number of buckets according to the data size. The following Exmaple shows an example of adding a partition to the above table with a new number of buckets.

~~~ SQL
ALTER TABLE site_access
ADD PARTITION p4 VALUES LESS THAN ("2020-04-31")
DISTRIBUTED BY HASH(site_id) BUCKETS 20;
~~~

-: Exmaple 3.4：Add a new partition to the site_access table with a new number of buckets

### How to select partition key

Each partition is a management unit that follows storage policies such as number of replications, hot/cold policy, storage media, etc. In most cases, the most recent data is more likely to be queried. To optimize query performance, users can leverage StarRocks’s partition trimming feature and place the most recent data within a partition to minimize the amount of data for scanning. Also, StarRocks supports using multiple storage media (SATA/SSD) within a cluster. Users can put the partition where the latest data is located on SSDs and take advantage of SSD's random read/write feature to improve query performance. The historical data can be put on a SATA disk to save the cost of data storage.

In most cases, users select the time column as the partition key, and set the number of partitions based on the data volume. The original data volume of a single partition is recommended to be maintained within 100G.

### How to choose the bucket key

StarRocks uses the hash algorithm as the bucketing algorithm. Within the same partition, data with the same bucket key value forms sub-tables (tablets). Tablet replicas are physically managed by separate local storage engines. Data import and query take place in tablet replicas. Tablets are the basic unit of data balancing and recovery.

In Exmaple 3.2, `site_access` uses `site_id` as the bucketing key because query requests use `site` as the filter. By using `site_id` as the bucketing key, a large number of irrelevant buckets can be trimmed off during querying. In the query in Exmaple 3.5 below, 9 out of 10 buckets can be trimmed off, and only 1/10 of the table `site_access` needs to be scanned.

~~~ SQL
select
city_code, sum(pv)
from site_access
where site_id = 54321;
~~~

:-: Exmaple 3.5: a query against table site\_access

However, there is a situation where the `site_id` distribution is assumed to be very uneven, that is,  a large amount of data comes from  a small number of  sites (power-law distribution, Pareto principle). If the user still uses the above bucketing method, the data distribution will be severely skewed, leading to local performance bottlenecks in the system. At this point, users need to adjust the bucket key to break up the data and avoid performance problems. In Exmaple 5 below, the combination of `site_id` and `city_code` can be used as the bucket key to divide the data more evenly.

~~~ SQL
CREATE TABLE site_access
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id,city_code) BUCKETS 10;
~~~

:-: Exmaple 3.6: Using site_id, city_code as bucketing keys

 Using `site_id` as the bucking key is good for short queries. It reduces the data exchange between nodes and improves the overall performance of the cluster. Using the combination of `site_id` and `city_code` as the bucketing key is good for long queries.  It takes advantage of the overall concurrency performance of the distributed cluster and Increases the throughput. Users choose the method that suits best to their use case.

### How to determine the number of buckets

In a StarRocks system, a partitioned bucket is a unit of actual physical file organization. After the data is written to disk, it will involve the management of disk files. Generally speaking, we do not recommend setting the buckets number too big or too small, it is more appropriate to try to be moderate.

As a rule of thumb, it is not recommended to have more than 10G per bucket, where 10G refers to the original data. Considering the compression ratio, the file size of each bucket on disk after compression is around 4~5G, which is sufficient to meet business needs in most cases.

Users are recommended to adjust the number of buckets when building tables according to the change of cluster size. The change of cluster size mainly refers to the change of the number of nodes. Suppose there are 100G of raw data, according to the above criteria, 10 buckets can be built. However, if the user has 20 machines, then the amount of data per bucket can be reduced and the number of buckets can be increased.

### Best Practices

When using StarRocks, the choice of partitioning and bucketing is very critical. Choosing a suitable partition key and bucket key when creating tables can effectively improve the overall performance of the cluster.  
Here are some suggestions for partitioning and bucketing selection in special application scenarios.

* **Data skew**: If the data is skewed, instead of using only one column, we recommend using a combination of multiple columns for data partitioning and bucketing.
* **High Concurrency**: Partitioning and bucketing should meet the query requirements at its best, which can effectively reduce the amount of data for scanning and improve concurrency.
* **High Throughput**: Break up the data to allow the cluster to scan the data with higher concurrency and complete the corresponding computation.

## Dynamic Partition Management

In real-world scenarios, the timeliness of data is important. New partitions need to be created, and expired partitions need to be deleted. StarRocks's dynamic partitioning mechanism enables partition rollover. StarRocks provides Time to Live (TTL) management to automatically add or delete partitions.

### Creating a table that supports dynamic partitioning

The following example shows the setup of dynamic partitioning.

~~~ SQL
CREATE TABLE site_access(
event_day DATE,
site_id INT DEFAULT '10',
city_code VARCHAR(100),
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(
PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
)
DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32
PROPERTIES(
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-3",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "32"
);
~~~

:-: Example 4.1: Tables in dynamic partitioning

To configure the dynamic partitioning policy, specify `PEROPERTIES` in the table creation statement in example 4.1. The configuration items can be described as follows.

* dynamic\_partition.enable: To enable dynamic partitioning, set it as `TRUE`. Otherwise, set it as `FALSE`. The default is `TRUE`.
* dynamic\_partition.time\_unit : The granularity of dynamic partition scheduling, specified as `DAY`/`WEEK`/`MONTH`.

  * Exmaple 1 shows an example of a partition by `day`, where the partition name suffix satisfies `yyyyMMdd`, e.g., 2030325.
 PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
 PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
 PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
 PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
  * When `WEEK` is specified, the partition name is created with the suffix format  `yyyy_ww`, e.g. 2020\_13
  * When `MONTH` is specified, the partition name is created with the suffix format `yyyyMM`, e.g. 202003.

* dynamic\_partition.start: The start time of the dynamic partition. Based on the day, partitions that exceed this time range will be deleted. If not filled, it defaults to `Integer.MIN\_VALUE` i.e. -2147483648.
* dynamic\_partition.end: The end time of the dynamic partition. A partition range of N units will be created ahead of time, based on the day.
* dynamic\_partition.prefix: The prefix of the dynamically created partition name.
* dynamic\_partition.buckets: The number of buckets corresponding to the dynamically created partition.

In Exmaple 4.1, a table is created with  dynamic partitioning enabled simultaneously. The interval of partitioning is 3 days before and after the current time (6 days in total). Assuming the current time is 2020-03-25, at each scheduling, partitions with upper bound less than 2020-03-22 will be deleted, and partitions for the next 3 days will be created at the same time. Once the scheduling is complete, new partitions will be shown as follows.

~~~ SQL
[types: [DATE]; keys: [2020-03-22]; ‥types: [DATE]; keys: [2020-03-23]; )
[types: [DATE]; keys: [2020-03-23]; ‥types: [DATE]; keys: [2020-03-24]; )
[types: [DATE]; keys: [2020-03-24]; ‥types: [DATE]; keys: [2020-03-25]; )
[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
~~~

:-: Exmaple 4.2: Partitioning of a table being automatically add/delete partitions

The scheduling depends on the FE configuration whose resident threads are controlled by the parameters `dynamic_partition_enable` and `dynamic_partition_check_interval_seconds`. Each time it is scheduled, it reads the properties of the dynamic partitioning table to determine whether to add or delete partitions.

### View the current partition of the table

When a dynamic partitioning table operates, partitions are added and deleted automatically. You can check the current partitioning status by the following command.

~~~ SQL
SHOW PARTITIONS FROM site_access;
~~~

:-: Exmaple 4.3 : site\_access current partition

### Modify the partitioning attributes of a table

The attributes of dynamic partitioning can be modified. If you need to start or stop dynamic partitioning, you can do so by `ALTER TABLE`.

~~~ SQL
ALTER TABLE site_access SET("dynamic_partition.enable"="false");
ALTER TABLE site_access SET("dynamic_partition.enable"="true");
~~~

Note: Similarly, you can also modify other properties accordingly.

### Caution

The dynamic partitioning approach relies on StarRocks to logically manage partitions. It is important to make sure that the partition name meets the requirements, otherwise it will fail to be created. The requirements are as follows.

* When `DAY` is specified, the partition name should be suffixed with `yyyyMMdd`, e.g., 2030325.
* When `WEEK` is specified, the partition name should be suffixed with `yyyy\_ww`, e.g. 2020\_13, for the 13th week of 2020.
* When `MONTH` is specified, the partition name should be suffixed with `yyyyMM`, e.g. 202003.

## Create and modify partitions in bulk

### Create date partitions in bulk during table creation

Users can create partitions in bulk by giving a `START` value, an `END` value and an `EVERY` clause defining the incremental value of the partitions.

The `START` value will be included whereas the `END` value will be excluded.

For example.

~~~ SQL
CREATE TABLE site_access (
 datekey DATE,
 site_id INT,
 city_code SMALLINT,
 user_name VARCHAR(32),
 pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (
 START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 day)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
~~~

StarRocks will automatically create the equivalent partitions as follows:

~~~ TEXT
PARTITION p20210101 VALUES [('2021-01-01'), ('2021-01-02')),
PARTITION p20210102 VALUES [('2021-01-02'), ('2021-01-03')),
PARTITION p20210103 VALUES [('2021-01-03'), ('2021-01-04'))
~~~

Currently, the partition key only supports date type and integer type. The partition type needs to match the expression in `EVERY`.

When the partition key is a date type, you need to specify the `INTERVAL` keyword to indicate the date interval. Currently, the supported keywords are`day`, `week`, `month`, and `year`. The partition naming conventions are the same as the ones of dynamic partitioning.

### Create numeric partitions in bulk during table creation

When the partition key is an integer type, numbers are used to perform the partition. Note that partition values need to be quoted in quotes, while `EVERY` does not need to be quoted. For example:

~~~ SQL
CREATE TABLE site_access (
 datekey INT,
 site_id INT,
 city_code SMALLINT,
 user_name VARCHAR(32),
 pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (
 START ("1") END ("5") EVERY (1)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
~~~

The above statement will produce the following partitions:

~~~ SQL
PARTITION p1 VALUES [("1"), ("2")),
PARTITION p2 VALUES [("2"), ("3")),
PARTITION p3 VALUES [("3"), ("4")),
PARTITION p4 VALUES [("4"), ("5"))
~~~

### Create different types of date partitions in bulk during table creation

StarRocks also supports defining different types of partitions at the same time when creating a table, as long as these partitions do not intersect. For example:

~~~ SQL
CREATE TABLE site_access (
 datekey DATE,
 site_id INT,
 city_code SMALLINT,
 user_name VARCHAR(32),
 pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (
 START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR),
 START ("2021-01-01") END ("2021-05-01") EVERY (INTERVAL 1 MONTH),
 START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(site_id) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
 
~~~

The above statement will produce the following partitions:

~~~ TEXT
PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01')),
PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
PARTITION p20210501 VALUES [('2021-05-01'), ('2021-05-02')),
PARTITION p20210502 VALUES [('2021-05-02'), ('2021-05-03')),
PARTITION p20210503 VALUES [('2021-05-03'), ('2021-05-04'))
~~~

### Create partitions in bulk after table being created

Similar to bulk partition creation during table creation, StarRocks also supports bulk partition creation via `ALTER` statement. Partitions are created by specifying the `ADD PARITIONS` keyword with the `START`, `END` and `EVERY` values.
