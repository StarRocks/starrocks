# Data distribution

Configuring appropriate partitioning and bucketing at table creation data can achieve even data distribution and improve query performance. Even data distribution means dividing the data into subsets according to certain rules and distributing them evenly across different nodes. It can also reduce the data scanned and make full use of the concurrency of the cluster during queries, thereby improving query performance.
> **NOTE**
>
> - Since v3.1, you can not specify the bucketing key in the DISTRIBUTED BY clause when creating a table or adding a partition. StarRocks supports random bucketing, which randomly distributes data across all buckets. For more information, see [Random bucketing](#random-bucketing-since-v31).
> - Since v2.5.7, you no longer need to manually set the number of buckets when you create a table or add a partition. StarRocks can automatically set the number of buckets (BUCKETS). However, if the performance does not meet your expectations after StarRocks automatically sets the number of buckets and you are familiar with the bucketing mechanism, you can still [manually set the number of buckets](#determine-the-number-of-buckets).

## Distribution methods

### Distribution methods in general

Modern distributed database systems generally use the following basic distribution methods: Round-Robin, Range, List, and Hash.

![Data distribution method](../assets/3.3.2-1.png)

- **Round-Robin**: distributes data across different nodes in a cyclic.
- **Range**: distributes data across different nodes based on the ranges of partitioning column values. As shown in the diagram, the ranges [1-3] and [4-6] correspond to different nodes.
- **List**: distributes data across different nodes based on the discrete values of partitioning columns, such as gender and province. Each discrete value is mapped to a node, and multiple different values might be mapped to the same node.
- **Hash**: distributes data across different nodes based on a hash function.

To achieve more flexible data partitioning, in addition to using one of the above data distribution methods, you can also combine these methods based on specific business requirements. Common combinations include Hash+Hash, Range+Hash, and Hash+List.

### Distribution methods in StarRocks

StarRocks supports both seperate and composite use of data distribution methods.

> **NOTE**
>
> In addition to the general distribution methods, StarRocks also supports Random distribution to simplify bucketing configuration.

Also, StarRocks distributes data by implementing the two level partitioning + bucketing method.

- The first level is partitioning: Data within a table can be partitioned. The partitioning method can be range partitioning or list partitioning. Or you can choose not to use partitioning (the entire table is regarded as one partition).
- The second level is bucketing: Data in a partition needs to be further distributed into smaller bucktes. The bucketing method can be hash or random bucketing.

| **Distribution method**      | **Partitioning and bucketing method**                            | **Description**                                                  |
| ------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Random distribution       | Random bucketing                                             | The entire table is considered a partition. Data in the table is randomly distributed into different buckets. This is the default data distribution method. |
| Hash distribution         | Hash bucketing                                               | The entire table is considered a partition. The data in the table is distributed to the corresponding bucket, which is based on the hash value of the data's bucketing key by using a hashing function. |
| Range+Random distribution | <ol><li>Expression partitioning or range partitioning </li><li>Random bucketing </li></ol> | <ol><li>The data of the table is partitioned based on the ranges of values in the partitioning columns. </li><li>The data in a partition is randomly distributed across different buckets. </li></ol> |
| Range+Hash distribution   | <ol><li>Expression partitioning or range partitioning</li><li>Hash bucketing </li></ol> | <ol><li>The data of the table is partitioned based on the ranges of values in the partitioning column.</li><li>The data in a partition is distributed to the corresponding bucket, which is based on the hash value of the data's bucketing key by using a hashing function.</li></ol> |
| List+Hash distribution    | <ol><li>Expression partitioning or List partitioning</li><li>Hash bucketing </li></ol> | <ol><li>The data in the table is partitioned based on the values in the partitioning column.</li><li>The data in a partition is distributed to the corresponding bucket, which is based on the hash value of the data's bucketing key by using a hashing function.</li></ol> |
| List+Random distribution  | <ol><li>Expression partitioning or List partitioning</li><li>Random bucketing </li></ol> | <ol><li>The data in the table is partitioned based on the values in the partitioning column.</li><li>The data in a partition is randomly distributed across different buckets.</li></ol> |

- **Random distribution**

  If you do not configure partitioning and bucketing methods at table creation, random distribution is used by default.

  ```SQL
  CREATE TABLE site_access1 (
      event_day DATE,
      site_id INT DEFAULT '10', 
      pv BIGINT DEFAULT '0' ,
      city_code VARCHAR(100),
      user_name VARCHAR(32) DEFAULT ''
  )
  DUPLICATE KEY (event_day,site_id,pv);
  -- Because the partitioning and bucketing methods are not configured, Random distribution is used by default (currently only support Deplicate Key tables).
  ```

- **Hash distribution**

  ```SQL
  CREATE TABLE site_access2 (
      event_day DATE,
      site_id INT DEFAULT '10',
      city_code SMALLINT,
      user_name VARCHAR(32) DEFAULT '',
      pv BIGINT SUM DEFAULT '0'
  )
  AGGREGATE KEY (event_day, site_id, city_code, user_name)
  -- Use hash bucketing as the bucketing method and must specify the bucketing key.
  DISTRIBUTED BY HASH(event_day,site_id); 
  ```

- **Range+Random distribution**

  ```SQL
  CREATE TABLE site_access3 (
      event_day DATE,
      site_id INT DEFAULT '10', 
      pv BIGINT DEFAULT '0' ,
      city_code VARCHAR(100),
      user_name VARCHAR(32) DEFAULT ''
  )
  DUPLICATE KEY(event_day,site_id,pv)
  -- Use expression partitioning as the partitioning method and configure a time function expression.
  -- You can also use range partitioning.
  PARTITION BY date_trunc('day', event_day);
  -- Because the bucketing method is not configured, Random bucketing is used by default (currently only support Deplicate Key tables).
  ```

- **Range+Hash distribution**

  ```SQL
  CREATE TABLE site_access4 (
      event_day DATE,
      site_id INT DEFAULT '10',
      city_code VARCHAR(100),
      user_name VARCHAR(32) DEFAULT '',
      pv BIGINT SUM DEFAULT '0'
  )
  AGGREGATE KEY(event_day, site_id, city_code, user_name)
  -- Use expression partitioning as the partitioning method and configure a time function expression.
  -- You can also use range partitioning.
  PARTITION BY date_trunc('day', event_day)
  -- Use hash bucketing as the bucketing method and must specify the bucketing key.
  DISTRIBUTED BY HASH(event_day, site_id);
  ```

- **List+Random distribution**

  ```SQL
  CREATE TABLE t_recharge_detail1 (
      id bigint,
      user_id bigint,
      recharge_money decimal(32,2), 
      city varchar(20) not null,
      dt date not null
  )
  DUPLICATE KEY(id)
  -- Use expression partitioning as the partitioning method and specify the partition column.
  -- You can also use list partitioning.
  PARTITION BY (city);
  -- Because the bucketing method is not configured, Random bucketing is used by default (currently only support Deplicate Key tables).
  ```

- **List+Hash distribution**

  ```SQL
  CREATE TABLE t_recharge_detail2 (
      id bigint,
      user_id bigint,
      recharge_money decimal(32,2), 
      city varchar(20) not null,
      dt date not null
  )
  DUPLICATE KEY(id)
  -- Use expression partitioning as the partitioning method and specify the partition column.
  -- You can also use list partitioning.
  PARTITION BY (city)
  -- Use hash bucketing as the bucketing method and must specify the bucketing key.
  DISTRIBUTED BY HASH(city,id); 
  ```

#### Partitioning

The partitioning method divides a table into multiple partitions. Partitioning primarily is used to split a table into different management units based on the partition key. You can set a storage strategy for each partition, including the number of bucketes, the strategy of storing hot and cold data, storage medium, and the number of replicas. StarRocks allows you to use multiple storage mediums within a cluster. For example, you can store the latest data on solid-state drives (SSD) to improve query performance, and historical data on SATA hard drives to reduce storage costs.

| Partitioning method                   | Scenarios                                                    | Methods to create partitions                       |
| ------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| Expression partitioning (recommended) | Previously known as automatic partitioning. This partitioning method is more flexible and user-friendly. It is suitable for most scenarios including querying and managing data based on continuous date ranges or emurated values. | Automatically created during data loading.         |
| Range partitioning                    | The typical scenario is to store simple and ordered data that is often queried and managed based on continuous date/numeric ranges. For instance, in some special cases, historical data needs to be partitioned by month, while recent data needs to be partitioned by day. |  Created manually, dynamically, or in batch|
| List partitioning                     | A typical scenario is to query and manage data based on emurated values, where a partition needs to include data with different values for each partition column. For example, if you frequently query and manage data based on country and city, you can use this method and select `city` as the partition column. So a partition can contain data for multiple cities belonging to one country. | Created manually                                    |

**How to choose partition columns and unit**

- Selecting a appropriate partition column can effectively reduce the amount of data scanned during queries. In most business systems, partitioning based on time is commonly chosen to optimize performance issues caused by deleting expired data and facilitate management of tiered storage of hot and cold data. In this case, you can use expression partitioning and range partitioning and specify a time column as the partition column. Additionally, if the data is frequently queried and managed based on emurated values, you can use expression partitioning and list partitioning and specify a column including these values as the partition column.
- When choosing the partitioning unit, you need to be consider factors such as data volume, query patterns, and data management granularity.
  - Example 1: If the volume of data per month in a table is small, partitioning by month can reduce the number of metadata entries compared to partitioning by day, thereby reducing the resource consumption of metadata management and scheduling.
  - Example 2: If the monthly data volume of a table is large and most query conditions are precise to the day, partitioning by day can effectively reduce the amount of data scanned during queries.
  - Example 3: If the data needs to expire on a daily basis, partitioning by day can be used.

#### Bucketing

The partitioning method divides a partition into multiple buckets. Data in a bucket is referred as a tablet.

Bucketing Method: StarRocks supports [random bucketing](#random-bucketing-since-v31) (from v3.1) and [hash bucketing](#hash-bucketing).

- Random bucketing: When creating a table or adding partitions, you do not need to set a bucketing key. Data within the same partition is randomly distributed into different buckets.

- Hash Bucketing: When creating a table or adding partitions, you need to specify a bucketing key. Data within the same partition is divided into buckets based on the bucketing key, and rows with the same value in the bucketing key are assigned to the corresponding and unique bucket.

Number of Buckets: By default, StarRocks automatically sets the number of buckets (from v2.5.7). And you can also manually set the number of buckets. For more information, please refer to [determining the number of buckets](#determine-the-number-of-buckets).

## Create and manage partitions

### Create partitions

#### Expression partitioning (recommended)

> **NOTICE**
>
> Since v3.1,  StarRocks's [shared-data mode](../deployment/deploy_shared_data.md) supports the time function expression and does not support the column expression.

Since v3.0, StarRocks supports [expression partitioning](./expression_partitioning.md) (previously known as automatic partitioning) which is more flexible and user-friendly. This partitioning method is suitable for most scenarios including querying and managing data based on continuous date ranges or emurated values.

You only need to configure a partition expression (either a time function expression or a column expression) at table creation, and StarRocks will automatically create partitions during data loading. You no longer need to manually create numerous partitions in advance, nor configure dynamic partition properties.

#### Range partitioning

Range partitioning is suitable for storing simple and contiguous data, such as time series data (dates or timestamps) or continuous numerical data. And you frequently query and manage data based on continuous date/numerical ranges. Also, it can be applied in some special cases where historical data needs to be partitioned by month, and recent data needs to be partitioned by day.

StarRocks stores data in the corresponding partition based on the explicit mapping of the predefined range for each partition.

**Dynamic partitioning**

[Dynamic partitioning](./dynamic_partitioning.md) related properties are configured at table creation. StarRocks automatically creates new partitions in advance and removes expired partitions to ensure data freshness and implement lifecycle management for partitions, known as Time to Life (TTL).

Different from the automatic partition creation ability provided by the expression partitioning, dynamic partitioning can only periodically creating new partitions based on the properties. If the new data does not belong to these partitions, an error is returned for the loading job. However, the automatic partition creation ability provided by the expression partitioning always can create corresponding new partitions based on the loaded data.

**Manually create partitions**

Using a suitable partition key can effectively reduce the amount of data scanned during queries. Currently, only date and integer data types are supported as partition columns. In business scenarios, partition keys are typically chosen from a data management perspective. Common partition columns include dates and regions.

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(
    PARTITION p1 VALUES LESS THAN ("2020-01-31"),
    PARTITION p2 VALUES LESS THAN ("2020-02-29"),
    PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
DISTRIBUTED BY HASH(site_id);
```

**Create multiple partitions at a time**

Partition a table by specifying START, END, and EVERY. You can  by using this method. For more information, see [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md).

Multiple partitions can be created at a time at and after table creation. You can specify the start and end time for all the partitions created at a time in `START()` and `END()` and the partition increment value in `EVERY()`. However, note that the range of partitions includes the start time but does not include the end time. The naming for partitions is the same as that of dynamic partitioning.

- **Partition a table on a date-type column (DATE and DATETIME) at the table creation**

  When the partition column is of date type, during table creation, you can use `START` and `END` to specify the start date and end date for all the partitions created at a time, and `EVERY` to specify the partition increment value. The `EVERY` uses the `INTERVAL` keyword to indicate the date interval, and currently supports date intervals to be `HOUR` (since version 3.0), `DAY`, `WEEK`, `MONTH`, and `YEAR`.

  In the following example, the date range of all the partitions created at a time starts from 2021-01-01 and ends on 2021-01-04, with an increment value of one day:

    ```SQL
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
        START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
    )
    DISTRIBUTED BY HASH(site_id)
    PROPERTIES ("replication_num" = "3" );
    ```

    It is equivalent to using the following `PARTITION BY` clause in the CREATE TABLE statement:

    ```SQL
    PARTITION BY RANGE (datekey) (
    PARTITION p20210101 VALUES [('2021-01-01'), ('2021-01-02')),
    PARTITION p20210102 VALUES [('2021-01-02'), ('2021-01-03')),
    PARTITION p20210103 VALUES [('2021-01-03'), ('2021-01-04'))
    )
    ```

- **Partition a table on a date-type column (DATE and DATETIME)  with different date intervals at the table creation**.

  You can create batches of date partitions with different date intervals by specifying different date intervals in `EVERY` for each batch of partitions (make sure that each batch partition intervals does not overlap). Partitions in each batch are created according to the defined date interval in the `EVERY clause`. For example:

    ```SQL
    CREATE TABLE site_access(
        datekey DATE,
        site_id INT,
        city_code SMALLINT,
        user_name VARCHAR(32),
        pv BIGINT DEFAULT '0'
    )
    ENGINE=olap
    DUPLICATE KEY(datekey, site_id, city_code, user_name)
    PARTITION BY RANGE (datekey) 
    (
        START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR),
        START ("2021-01-01") END ("2021-05-01") EVERY (INTERVAL 1 MONTH),
        START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
    )
    DISTRIBUTED BY HASH(site_id)
    PROPERTIES(
        "replication_num" = "3"
    );
    ```

    It is equivalent to using the following `PARTITION BY` clause in the CREATE TABLE statement:

    ```SQL
    PARTITION BY RANGE (datekey) (
    PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01')),
    PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
    PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
    PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
    PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
    PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
    PARTITION p20210501 VALUES [('2021-05-01'), ('2021-05-02')),
    PARTITION p20210502 VALUES [('2021-05-02'), ('2021-05-03')),
    PARTITION p20210503 VALUES [('2021-05-03'), ('2021-05-04'))
    )
    ```

- **Partition a table on an integer type column at the table creation**

  When the data type of the partition column is INT, you specify the range of partitions in  `START` and `END` and define the incremental value in `EVERY`. Example:

  > **NOTE**
  >
  > Do not double quote the incremental value in `EVERY`.

  In the following example, the range of all the partition starts from `1` and ends at `5`, with a partition increment of `1`:

    ```SQL
    CREATE TABLE site_access (
        datekey INT,
        site_id INT,
        city_code SMALLINT,
        user_name VARCHAR(32),
        pv BIGINT DEFAULT '0'
    )
    ENGINE=olap
    DUPLICATE KEY(datekey, site_id, city_code, user_name)
    PARTITION BY RANGE (datekey) (START ("1") END ("5") EVERY (1)
    )
    DISTRIBUTED BY HASH(site_id)
    PROPERTIES ("replication_num" = "3");
    ```

    It is equivalent to using the following `PARTITION BY` clause in the CREATE TABLE statement:

    ```SQL
    PARTITION BY RANGE (datekey) (
    PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01')),
    PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
    PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
    PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
    PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
    PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
    PARTITION p20210501 VALUES [('2021-05-01'), ('2021-05-02')),
    PARTITION p20210502 VALUES [('2021-05-02'), ('2021-05-03')),
    PARTITION p20210503 VALUES [('2021-05-03'), ('2021-05-04'))
    )
    ```

**Create partitions at a time after a table is created.**

  After a table is created, you can use the ALTER TABLE statement to add partitions at a time for the table. The syntax is similar to creating multiple partitions at a time at table creation. You can configure `START`, `END`, and `EVERY` in the `ADD PARTITIONS` clause to create multiple partitions at a time.

  ```SQL
  ALTER TABLE site_access 
  ADD PARTITIONS START ("2021-01-04") END ("2021-01-06") EVERY (INTERVAL 1 DAY);
  ```

#### List partitioning (since v3.1)

[List Partitioning](./list_partitioning.md) is suitable for accelerating queries and efficiently managing data based on emurated values. It is especially useful for scenarios where a partition needs to include data with different values in a partition column. For example, if you frequently query and manage data based on countries and cities, you can use this partitioning method and select the `city` column as the partition column. In this case, one partition can contain data of various cities belonging to one country.

StarRocks stores data in the corresponding partitions based on the explicit mapping of the predefined list of values for each partition.

### Manage  partitions

#### Add partitions

For range partitioning and list partitioning, you can manually add new partitions to store new data in a table. However for expression partitioning, because partitions are created autocmatically during data loading, you do not need to do so.

The following statement adds a new partition to table `site_access` to store data for a new month:

```SQL
ALTER TABLE site_access
ADD PARTITION p4 VALUES LESS THAN ("2020-04-30")
DISTRIBUTED BY HASH(site_id);
```

#### Delete a partition

The following statement deletes partition `p1` from table `site_access`.
> **NOTE**
>
> This operation does not immediately delete data in a partition. Data is retained in the Trash for a period of time (one day by default). If a partition is mistakenly deleted, you can use the [RECOVER](../sql-reference/sql-statements/data-definition/RECOVER.md) command to restore the partition and its data.

```SQL
ALTER TABLE site_access
DROP PARTITION p1;
```

#### Restore a partition

The following statement restores partition `p1` and data to table `site_access`.

``SQL
RECOVER PARTITION p1 FROM site_access;
``

#### View partitions

The following statement displays all partitions in table `site_access`.

```SQL
SHOW PARTITIONS FROM site_access;
```

## Configure bucketing

### Random bucketing (since v3.1)

StarRocks distributes the data in a partition randomly across all bucketss. It is suitable for scenarios with relatively small data sizes and relatively low requirement for query performance. If you do not set a bucketing method, StarRocks uses random bucketing by default and automatically sets the number of buckets.

However, note that if you are querying massive amounts of data and frequently using certain columns in the filtering condition, the query performance provided by Random Bucketing may not be optimal. In such scenarios, it is recommended to use [hash bucketing](#hash-bucketing). When querying with these columns in the filtering condition, you only need to scan and compute data in a small number of buckets that the query involves, which can significantly improve query performance.

**Precautions**

- You can only use random bucketing to create a Depulicate Key table.
- You can not specify a table bucketed randomly to belong to a [Colocation Group](../using_starrocks/Colocate_join.md).
- [Spark Load](../loading/SparkLoad.md) can not be used to load data into tables bucketed randomly.

In the following example, a table `site_access1` is created by using random bucketing, and the system automatically sets the number of buckets.

In the following CREATE TABLE example, the `DISTRIBUTED BY xxx` statement is not used, so StarRocks uses random bucketing by default, and utomatically the number of buckets.

```SQL
CREATE TABLE site_access1(
    event_day DATE,
    site_id INT DEFAULT '10', 
    pv BIGINT DEFAULT '0' ,
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT ''
)
DUPLICATE KEY(event_day,site_id,pv);
```

However, if you are familiar with StarRocks' bucketing mechanism, you can also manually set the number of buckets when creating a table with random bucketing.

```SQL
CREATE TABLE site_access2(
    event_day DATE,
    site_id INT DEFAULT '10', 
    pv BIGINT DEFAULT '0' ,
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT ''
)
DUPLICATE KEY(event_day,site_id,pv)
DISTRIBUTED BY RANDOM BUCKETS 8; -- manually set the number of buckets to 8
```

### Hash bucketing

StarRocks can use hash bucketing to subdivide data in a partition into bucktes based on the bucketing key and [the number of buckets](#determine-the-number-of-buckets). In Hash bucketing, a a hash function takes specific column value as input and calculates a hash value, which is then used to store the data to the corresponding bucket.

Advantages:

- Improved query performance: Rows with the same bucketing key values are stored in the same bucket, reducing the amount of data scanned during queries.

- Even data distribution: By selecting columns with higher cardinality (a larger number of unique values) as the bucketing key, data can be more evenly distributed into each bucket.

**How to choose the bucketing columns**

We recommend that you choose the column that satisfy the following two requirements as the bucketing column.

- high cardinality column such as ID
- column that often used in a filter for queries

But if the column that satisfies both requirements does not exist, you need to determine the buckting column according to the complexity of queries.

- If the query is complex, it is recommended that you select the high cardinality column as the bucketing column to ensure that the data is as evenly distributed as possible in each bucket and improve the cluster resource utilization.
- If the query is relatively simple, then it is recommended to select the column that is often used as in the query condition as the bucketing column to improve the query efficiency.

If partition data cannot be evenly distributed into each tablet by using one bucketing column, you can choose multiple bucketing columns. but it is recommended to use no more than 3 columns.

**Precautions**

- **When a table is created, you must specify the bucketing columns**.
- The values of bucketing columns cannot be updated.
- Bucketing columns cannot be modified after they are specified.

**Examples**

In the following example, the `site_access` table is created by using `site_id` as the bucketing column. Because this column is high cardinality column and is always used as a filter in queries. When the bucketing column `site_id` is used as a filter in queries, StarRocks only scans the relevant tablets, which greatly improves query performance.

```SQL
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
    PARTITION p1 VALUES LESS THAN ("2020-01-31"),
    PARTITION p2 VALUES LESS THAN ("2020-02-29"),
    PARTITION p3 VALUES LESS THAN ("2020-03-31")
)
DISTRIBUTED BY HASH(site_id);
```

Suppose each partition of table `site_access` has 10 buckets. In the following query, 9 out of 10 buckets are pruned, so StarRocks only needs to scan 1/10 of the data in the `site_access` table:

```SQL
select sum(pv)
from site_access
where site_id = 54321;
```

However, if `site_id` is unevenly distributed and a large number of queries are destined for only a few sites, the preceding bucketing method will cause severe data skew, causing system performance bottlenecks. In this case, you can use a combination of bucketing columns. For example, the following statement uses `site_id` and `city_code` as bucketing columns.

```SQL
CREATE TABLE site_access
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code, user_name)
DISTRIBUTED BY HASH(site_id,city_code);
```

In practical use, you can use one or two bucketing columns based on the characteristics of your business. Using one bucketing column `site_id` is highly beneficial for short queries as it reduces data exchange between nodes, thereby enhancing the overall performance of the cluster. On the other hand, adopting two bucketing columns `site_id` and `city_code` is advantageous for long queries as it can leverage the overall concurrency of the distributed cluster to significantly improve performance.

> **NOTE**
>
> - Short queries often invlove scanning a small amount of data, and can be completed on a single node.
> - Long queries, on the other hand, involve scanning a large amount of data, and their performance can be significantly improved by parallel scanning across multiple nodes in a distributed cluster.

### Determine the number of buckets

Buckets reflect how data files are organized in StarRocks.

- How to set the number of buckets when creating a table
  - Method 1: automatically set the number of buckets (Recommended)

    Since v2.5.7, StarRocks supports automatically setting the number of buckets based on machine resources and data volume for a partition.

    Example:

      ```SQL
      CREATE TABLE site_access(
          site_id INT DEFAULT '10',
          city_code SMALLINT,
          user_name VARCHAR(32) DEFAULT '',
          pv BIGINT SUM DEFAULT '0'
      )
      AGGREGATE KEY(site_id, city_code, user_name)
      DISTRIBUTED BY HASH(site_id,city_code); -- do not need to set the number of buckets
      ```

    To enable this feature, make sure that the FE dynamic parameter `enable_auto_tablet_distribution` is set to `TRUE`. After a table is created, you can execute [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20VIEW.md) to view the bucket number automatically set by StarRocks.

  - Method 2: manually set the number of buckets

    StarRocks 2.4 and later versions support using multiple threads to scan a tablet in parallel during a query, thereby reducing the dependency of scanning performance on the tablet count. We recommend that each tablet contain about 10 GB of raw data. If you intend to manually set the number of buckets, you can estimatethe the amount of data in each partition of a table and then decide the number of tablets. To enable parallel scanning on tablets, make sure the `enable_tablet_internal_parallel` parameter is set to `TRUE` globally for the entire system (`SET GLOBAL enable_tablet_internal_parallel = true;`).

    ```SQL
    CREATE TABLE site_access (
        site_id INT DEFAULT '10',
        city_code SMALLINT,
        user_name VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0')
    AGGREGATE KEY(site_id, city_code, user_name)
    -- Suppose the amount of raw data that you want to load into a partition is 300 GB.
    -- Because we recommend that each tablet contain 10 GB of raw data, the number of buckets can be set to 30.
    DISTRIBUTED BY HASH(site_id,city_code) BUCKETS 30;
    ```

- How to set the number of buckets when adding a partition
  - Method 1: automatically set the number of buckets (Recommended)

    Since v2.5.7, StarRocks supports automatically setting the number of buckets based on machine resources and data volume for a partition. To enable this feature, make sure that the FE dynamic parameter `enable_auto_tablet_distribution` retains the default value `TRUE`.

    To disable this feature, run the `ADMIN SET FRONTEND CONFIG ('enable_auto_tablet_distribution' = 'false');` statement. And when a new partition is added without specifying the number of buckets, the new partition inherits the the number of buckets set at the creation of the table. After a new partition is added successfully, you can execute SHOW PARTITIONS to view the number of buckets automatically set by StarRocks for the new partition.
  - Method 2: manually set the number of buckets

    You can also manually specify the bucket count when adding a new partition. To calculate the number of buckets for a new partition, you can refer to the approach used when manually setting the number of buckets at table creation, as mentioned above.

      ```SQL
      -- Manually create partitions
      ALTER TABLE <table_name> 
      ADD PARTITION <partition_name>
          [DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]];
          
      -- Dynamic partitioning
      ALTER TABLE <table_name> 
      SET ("dynamic_partition.buckets"="xxx");
      ```

> **NOTICE**
>
> You cannot modify the number of buckets for an existing partition.
