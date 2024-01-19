---
displayed_sidebar: "English"
---

# Use HLL for approximate count distinct

## Background

In a real-world scenario, the pressure to de-duplicate the data increases as the volume of data increases. When the size of data reaches a certain level, the cost of accurate de-duplication is relatively high. In this case, users usually use approximate algorithms to reduce the computational pressure. HyperLogLog (HLL), which will be introduced in this section, is an approximate de-duplication algorithm that has excellent space complexity O(mloglogn) and time complexity O(n). What’s more, the error rate of the computation result can be controlled to about 1%-10%, depending on the size of the data set and the hash function used.

## What is HyperLogLog

HyperLogLog is an approximate de-duplication algorithm that consumes very little storage space. The **HLL type** is used for implementing the HyperLogLog algorithm. It holds the intermediate results of the HyperLogLog calculation and can only be used as an indicator column type for data tables.

Since the HLL algorithm involves a lot of mathematical knowledge, we will use a practical example to illustrate it. Suppose we design a randomized experiment A, that is to do independent repetitions of coin flips until the first head; record the number of coin flips for the first head as a random variable X, then:

* X=1, P(X=1)=1/2
* X=2, P(X=2)=1/4
* ...
* X=n, P(X=n)=(1/2)<sup>n</sup>

We use test A to construct randomized test B which is to do N independent repetitions of test A, generating N independent identically distributed random variables X<sub>1</sub>, X<sub>2</sub>, X<sub>3</sub>, ..., X<sub>N</sub>.Take the maximum value of the random variables as X<sub>max</sub>. Leveraging the  great likelihood estimation, the estimated value of N is 2<sup>X<sub>max</sub></sup>.
<br/>

Now, we simulate the above experiment using the hash function on the given dataset:

* Test A: Calculate the hash value of dataset elements and convert the hash value to binary representation. Record the occurrence of bit=1, starting from the lowest bit of the binary.
* Test B: Repeat the Test A process for dataset elements of Test B. Update the maximum position "m" of the first bit 1 occurrence for each test;
* Estimate the number of non-repeating elements in the dataset as m<sup>2</sup>.

In fact, the HLL algorithm divides the elements into K=2<sup>k</sup> buckets based on the lower k bits of the element hash. Count the maximum value of the first bit 1 occurrence from the k+1st bit as m<sub>1</sub>, m<sub>2</sub>,..., m<sub>k</sub>, and estimate the number of non-repeating elements in the bucket as 2<sup>m<sub>1</sub></sup>, 2<sup>m<sub>2</sub></sup>,..., 2<sup>m<sub>k</sub></sup>. The number of non-repeating elements in the data set is the summed average of the number of buckets multiplied by the number of non-repeating elements in the buckets: N = K(K/(2<sup>\-m<sub>1</sub></sup>+2<sup>\-m<sub>2</sub></sup>,..., 2<sup>\-m<sub>K</sub></sup>)).
<br/>

HLL multiplies the correction factor with the estimation result to make the result more accurate.

Refer to the article [https://gist.github.com/avibryant/8275649](https://gist.github.com/avibryant/8275649) on implementing HLL de-duplication algorithm with StarRocks SQL statements:

~~~sql
SELECT floor((0.721 * 1024 * 1024) / (sum(pow(2, m * -1)) + 1024 - count(*))) AS estimate
FROM(select(murmur_hash3_32(c2) & 1023) AS bucket,
     max((31 - CAST(log2(murmur_hash3_32(c2) & 2147483647) AS INT))) AS m
     FROM db0.table0
     GROUP BY bucket) bucket_values
~~~

This algorithm de-duplicates col2 of db0.table0.

* Use the hash function `murmur_hash3_32` to calculate the hash value of col2 as a 32-signed integer.
* Use 1024 buckets, the correction factor is 0.721, and take the lower 10 bits of the hash value as the subscript of the bucket.
* Ignore the sign bit of the hash value, start from the next highest bit to the lower bit, and determine the position of the first bit 1 occurrence.
* Group the calculated hash values by bucket, and use the `MAX` aggregation to find the maximum position of the first bit 1 occurrence in the bucket.
* The aggregation result is used as a subquery, and the summed average of all bucket estimates is multiplied by the number of buckets and the correction factor.
* Note that the empty bucket count is 1.

The above algorithm has a very low error rate when the data volume is large.

This is the core idea of the HLL algorithm. Please refer to the [HyperLogLog paper](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) if you are interested.

### How to use HyperLogLog

1. To use HyperLogLog de-duplication, you need to set the target indicator column type to `HLL` and the aggregation function to `HLL_UNION` in the table creation statement.
2. Currently, only the aggregate table supports HLL as indicator column type.
3. When using `count distinct` on columns of the HLL type, StarRocks will automatically convert it to the `HLL_UNION_AGG` calculation.

#### Example

First, create a table with **HLL** columns, where uv is an aggregated column, the column type is `HLL`, and the aggregation function is [HLL_UNION](../sql-reference/sql-functions/aggregate-functions/hll_union.md).

~~~sql
CREATE TABLE test(
        dt DATE,
        id INT,
        uv HLL HLL_UNION
)
DISTRIBUTED BY HASH(ID);
~~~

> * Note: When the data volume is large, it is better to create a corresponding rollup table for high frequency HLL queries

Load data using [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md):

~~~bash
curl --location-trusted -u <username>:<password> -H "label:label_1600997542287" \
    -H "column_separator:," \
    -H "columns:dt,id,user_id, uv=hll_hash(user_id)" -T /root/test.csv http://starrocks_be0:8040/api/db0/test/_stream_load
{
    "TxnId": 2504748,
    "Label": "label_1600997542287",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 5,
    "NumberLoadedRows": 5,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 120,
    "LoadTimeMs": 46,
    "BeginTxnTimeMs": 0,
    "StreamLoadPutTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 29,
    "CommitAndPublishTimeMs": 14
}
~~~

Broker Load mode:

~~~sql
LOAD LABEL test_db.label
 (
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file")
    INTO TABLE `test`
    COLUMNS TERMINATED BY ","
    (dt, id, user_id)
    SET (
      uv = HLL_HASH(user_id)
    )
 );
~~~

Querying data

* The HLL column does not allow direct query of its original value, use the function [HLL_UNION_AGG](../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md) to query.
* To find the total uv,

`SELECT HLL_UNION_AGG(uv) FROM test;`

This statement is equivalent to

`SELECT COUNT(DISTINCT uv) FROM test;`

* Query uv of everyday

`SELECT COUNT(DISTINCT uv) FROM test GROUP BY ID;`

### Cautions

How should I choose between Bitmap and HLL? If the base of the dataset is in the millions or tens of millions, and you have a few dozen machines, use `count distinct`. If the base is in the hundreds of millions and needs to be accurately de-duplicated, use`Bitmap`; if approximate de-duplication is acceptable, use the `HLL` type.

Bitmap only supports TINYINT, SMALLINT, INT, and BIGINT. Note that LARGEINT is not supported. For other types of data sets to be de-duplicated, a dictionary needs to be built to map the original type to an integer type.  Building a dictionary  is complex, and requires a trade-off between data volume, update frequency, query efficiency, storage, and other issues. HLL does not need a dictionary, but it needs the corresponding data type to support the hash function. Even in an analytical system that does not support HLL internally, it is still possible to use the hash function and SQL to implement HLL de-duplication.

For common columns, users can use the NDV function for approximate de-duplication. This function returns an approximate aggregation of COUNT(DISTINCT col) results, and the underlying implementation converts the data storage type to the HyperLogLog type for calculation. The NDV function consumes a lot of resources  when calculating and is therefore  not well suited for high concurrency scenarios.

If you want to perform user behavior analysis, you may consider IntersectCount or custom UDAF.
