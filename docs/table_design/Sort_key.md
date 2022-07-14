# Sort Keys

## How Sort Keys Work

In StarRocks, data is sorted by the columns which are specified as sort keys to speed up queries. The sort key in the **duplicate model** is the column specified by `DUPLICATE KEY`. In the **aggregation model**, the sort key is the column specified by `AGGREGATE KEY`, and the sort key in the **update model** is the column specified by `UNIQUE KEY`. In the following Figure 5.1, `site_id`, `city_code` are the sort keys  in the table building statement.

~~~SQL
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
 
CREATE TABLE site_access_aggregate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
 
CREATE TABLE site_access_unique
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
UNIQUE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: Figure 5.1: Using sort keys for the three table building models

In Figure 5.1, the data in each table are sorted by `site_id` and `city_code`.

Notes:

1. In the table building statements, the sort keys must be defined  before the definitions of the other columns. Take the table building statement in Figure 1 as an example, the sort key of the three tables can be `site_id`, `city_code`, or `site_id`, `city_code`, `user_name`, but not `city_code`, `user_name`, or `site_id`, `city_code`, `pv`.
2. The sorting order of the sort keys is determined by the column order in the `CREATE TABLE` statement. The order of `DUPLICATE/UNIQUE/AGGREGATE KEY` needs to be consistent with the `CREATE TABLE` statement. Take table `site_access_duplicate` as an example, the following table building statement will throw an error.

~~~ SQL
-- Wrong table building statement
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(city_code, site_id)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
 
-- Correct table building statement
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: Figure 5.2: DUPLICATE KEY columns are not in the same sorting order as in CREATE TABLE

Let's look at what you can achieve by using sort keys in a query. Under different situations, the sort keys in Figure 1 can affect your query performance in different ways:

1.If the query contains both`site_id` and `city_code` in its`where` clause, the data rows scanned will be significantly reduced. For example:

~~~ SQL
select sum(pv) from site_access_duplicate where site_id = 123 and city_code = 2;
~~~

2.If the query contains only the `site_id` in its `where` clause, it can also locate the matching data rows efficiently. For example:

~~~ SQL
select sum(pv) from site_access_duplicate where site_id = 123;
~~~

3.If the query contains only the `city_code` in its `where` clause, all the data rows need to be scanned, and the performance will deteriorate. For example:

~~~ SQL
select sum(pv) from site_access_duplicate where city_code = 2;
~~~

In the first case, a binary search will be performed to find the specified range. If the table contains a great number of rows, doing a binary search directly on `site_id` and `city_code` needs to load both columns into the memory. This will consume a lot of memory space. As an optimization, StarRocks introduces sparse **shortkey indices** based on sort keys. The size of the sort index will be 1024 times smaller than the original size of data, so it will be fully cached in the memory, which effectively speeds up the query during the actual lookups. When there is a large number of sort key columns, it will still consume too muchmemory. To avoid it, we have the following restrictions on the shortkey indices:

* A shortkey column can only be the prefix of s sort key;
* There can only be at most 3 shortkey columns;
* A short key column cannot exceed 36 bytes;
* The short key columns cannot be columns of type FLOAT/DOUBLE;
* There can be at most one VARCHAR columns, and it can only appear at the end;
* The length of shortkey can exceed 36 bytes when the last column of shortkey index is of CHAR or VARCHAR type;
* The above restrictions don’t apply when user specifies `PROPERTIES {short_key = "integer"}` in the table building statement.

### 3.4.2 How to choose the sort key

It is very important to choose the right sort key to accelerate queries. For example, if the user only selects `city_code` as the query condition when querying the table `site_access_duplicate`, it results in bad query performance. You should make the columns that are frequently used as query conditions as sort keys.

When there are multiple sort key columns, the order of the sort key columns also matters. The columns with high value distribution and are frequently used in queries should be placed at the front. In the table `site_access_duplicate`, `city_code` only allows a fixed set of values (the number of cities is fixed), while the number of possible values of `site_id` is much larger than `city_code` and keeps growing. So, the value distribution of `site_id` is much higher than `city_code`.

Still use the table `site_access_duplicate` as an example:

* If a user frequently use both `site_id` and `city_code` in queries, having `site_id` as the first sort key column is more efficient.
* If a user queries by `city_code` more frequently than usingthe combination of `site_id` and `city_code`, then having `city_code` as the first sort key column is more appropriate.
* If querying by `city_code` and by `city_code`+`site_id` are both frequent use cases, querying by `city_code` alone may trigger a full scan of all the row and result in deteriorated performance. In this case, it would be more efficient to create a RollUp table with `city_code` as the first column. The RollUp table will then build another Sort Index for `city_code` to speed up the query.`

### 3.4.3 Notes

Since the shortkey index has a fixed size in StarRocks (36 bytes), memory bloating won’t be an issue. However, you should keep the following 4 rules in mind.

1. The sort key columns must start from the first table column and have to be consecutive.
2. The order of the sort keys is determined by the order of the columns defined in the create table statement.
3. Do not use too many sort key columns. If a large number of columns are selected as sort keys, the sorting overhead will result in an increased of data import overhead.
4. Oftentimes the first few sort key columns can already locate the range of the rows. Adding more sort key columns will not improve the query performance.
