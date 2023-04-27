# FAQ

## Deployment

### How to select hardware and optimize configuration

#### Hardware Selection

* For BE, we recommend 16 cores with 64GB or more. For FE, we recommend 8 cores with 16GB or more.
* HDDs or SSDs can be used.
* CPU must support AVX2 instruction sets, use `cat /proc/cpuinfo |grep avx2` to confirm there is output. If not, we recommend replacing the machine. StarRocks' vectorization engine needs CPU instruction sets to perform a better effect.
* The network needs 10 GB NIC and 10 GB switch.

## Modeling

### Partitioning and bucketing

#### Range partitioning

* Reasonable range partitioning can reduce the amount of data for scanning. Taking a data management perspective, we normally choose "time" or "region" as range partition keys.
* With dynamic partitioning, you can create partitions automatically at regular intervals (on a daily basis).

#### Hash partitioning

* Choose **high-cardinality columns** as the hash partition key to ensure that data is balanced among buckets. If a column has a unique ID, use it as the hash partition key. If there is data skew, use multiple columns as the hash partition key but try not to choose too many columns.
* The number of buckets affects query parallelism. We recommend setting each bucket around 100MB to 1GB.
* To make full use of the limited machine resources, set the number of buckets based on ` Number of BE * cpu core / 2 `. For example, you have a table with 100GB data and four BEs each of which is 64C. To take full advantage of the CPU resources with only one partition, you can set 144 buckets (`4 * 64 /2  = 144`) and each bucket contains 694 MB data.

### Sort key

* Design the sort key based on your query needs.
* To speed up queries, choose columns **that are often used as filter and group by conditions** as sort keys.
* If there is **a large data-point query**, we recommend you to put the query ID in the first column. For example, if the query is `select sum(revenue) from lineorder where user_id='aaa100'`; and there is high concurrency, we recommend putting `user\_id` as the first column of the sort key.
* If the query is mainly **aggregation and scan**, we recommend putting the low-cardinality columns first. For example, if the main type of query is `select region, nation, count(*) from lineorder_flat group by region, nation`, it would be more appropriate to put region as the first column and nation as the second. Putting the low-cardinality columns in front achieves data locality.

### Data types

* Choose precise data types. In other words, don't use string if you can use int; don't use bigint if you can use int. Precise data types help the database perform better.

## Query

### Query parallelism

* Set the query parallelism via the session variable `parallel_fragment_exec_instance_num`. If the query performance is not satisfying but CPU resources are sufficient, adjust the parallelism by setting `parallel_fragment_exec_instance_num = 16;`. Parallelism can be set to **half the number of CPU cores** .
* To make the session variable globally valid, set**global** `parallel_fragment_exec_instance_num = 16;`.
* `parallel_fragment_exec_instance_num` is affected by the number of tablets owned by each BE. For example, if a table has 32 tablets and 3 partitions distributed on 4 BEs, then the number of tablets per BE is `32 * 3 / 4 = 24`. In this case, the parallelism value of each BE cannot exceed 24. Even if you set `parallel_fragment_exec_instance_num = 32`, the parallelism value will still be 24 during execution.
* To process high QPS queries, we recommend setting  `parallel_fragment_exec_instance_num` to `1`. This reduces the competition for resources during querying and therefore improves the QPS.

### Use profile to analyze query bottlenecks

* To view the query plan, use the command `explain sql`.
* To enable profile reporting, set `enable_profile = true`.
* To view the current query and profile information, go to `http:FE_IP:FE_HTTP_PORT/queries`.
