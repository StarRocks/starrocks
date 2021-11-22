# Data Models

According to the mapping relationship between ingested data and actual stored data, StarRocks provides corresponding data models for different types of tables. Here we reference the concept of primary key (in the relational model), and use the value of the key column as the sort key. Compared with the traditional primary key,StarRocks’s sort key has these features:

* All key columns are sort keys, so the sort key mentioned later is essentially a key column.
* Sort keys are repeatable and do not have to satisfy the uniqueness constraint.
* Each column of the table is stored by cluster in the order of the sorted keys.
* Sort keys use sparse indexes.

There are three models that can be used to ingest multiple rows of data with duplicate primary keys and populate them to data tables:

* Duplicate model: There are rows of data with duplicate primary keys in the table. The stored data and ingested data rows correspond to each other, so that users can recall all historical data.
* Aggregation model: There are no data rows with duplicate primary keys. In this case, those rows are merged into one row according to the aggregation function. Users can recall the aggregated results of all historical data, but not the data itself.
* Update model: This is a special case of aggregation model. In this model, the primary key satisfies the uniqueness constraint, and the most recently ingested row replaces the other rows that have duplicate primary keys. This is equivalent to an aggregation model in which the aggregation function is `REPLACE`, and the `REPLACE` function returns the most recent data in the set.

Note:

* In the table build statement, the definition of the sort key must appear before the definition of the value column.
* The order of key columns appearing in the build statement determines the order of sort keys presented.
* The shortkey index contains several prefix columns of the sort key.

## Duplicate model

### Scenarios for duplicate model

Duplicate model is the default model in StarRocks.

The suitable scenarios have the following characteristics:

* Need to retain the original data (e.g. logs, operational records, etc.) for analysis.
* Be able to query flexibly and not limited to pre-defined analytical methods, which are difficult to accomplish by traditional pre-aggregation methods.
* Infrequent data updates. The source of the imported data is usually log data or time-series data, which is mainly characterized by append write and the data itself will not change much after it is generated.

### Principle of duplicate model

Users can specify the sort key of the table. If it’s not explicitly specified, StarRocks will set default columns as the sort key. When the sort key is set as a filter, StarRocks can quickly filter the data and reduce query latency.

Note: When importing two identical rows of data into a duplicate model table, StarRocks will consider them as two rows of data.

### How to use duplicate model

The table adopts the duplicate model by default. The sort key uses shortkey index to filter the data quickly. Users can consider setting the frequently used key column as sort key. For example, if you frequently view data for a certain type of event in a certain time range, you can set `event time` and `event type` as sort keys.

The following is an example of creating a data table using the duplicate model.

* Where `DUPLICATE KEY(event_time, event_type)` indicates that the duplicate model is used, and the sort key is defined before the definitions of other columns.

~~~sql
CREATE TABLE IF NOT EXISTS detail (
 event_time DATETIME NOT NULL COMMENT "datetime of event",
 event_type INT NOT NULL COMMENT "type of event",
 user_id INT COMMENT "id of user"
 device_code INT COMMENT "device of",
 channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
~~~

### Notes on duplicate model

Make full use of the sort key by defining the frequently used columns up front to improve query speed. In the duplicate model, you can specify some of the key columns as sort keys, whereas in the aggregation model and update model, the sort keys can only be all key columns.

## Aggregation model

### Scenarios for aggregation model

Data statistics and aggregation are common concepts of data analysis. For example:

* Website or APP owners monitor data on total visits and visit duration
* Advertising vendors gather data onad clicks, total number of displays, and consumption statistics for advertisers;
* Marketing people analyze data on annual transactions and top products of each demographic category for a specific quarter or month.

The suitable scenarios for  using aggregation model have the following characteristics:

* Queries need to be aggregated through `sum`, `count`, and/or `max`.
* No need to recall the original data.
* Historical data will not be updated frequently;new data will be appended.

### Principle of aggregation model

StarRocks aggregates value columns by corresponding key columns, which reduces the amount of data for processing and increases query efficiency.

Take the following raw data as an example.

| Date | Country | PV |
| :---: | :---: | :---: |
| 2020.05.01 | CHN | 1 |
| 2020.05.01 | CHN | 2 |
| 2020.05.01 | USA | 3 |
| 2020.05.01 | USA | 4 |

In the StarRocks aggregation model, the four rows of data are converted to two rows, which reduces the amount of data processed during the subsequent query.

|   Date  |   Country  |   PV  |
| :---: | :---: | :---: |
|  2020.05.01   |  CHN   |  3   |
|  2020.05.01   |  USA   |  7   |

### How to use aggregation model

When creating a table, the aggregation model is enabled by specifying the aggregation function for the value column. Users can use the `AGGREGATE KEY` to define the sort key.

The following is an example of creating a data table using the aggregation model.

* `site_id`, `date` and `city_code` are sort keys;
* `pv` is the value column, using the aggregation function `SUM`.

~~~sql
CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (
 site_id LARGEINT NOT NULL COMMENT "id of site",
 date DATE NOT NULL COMMENT "time of event",
 city_code VARCHAR(20) COMMENT "city_code of user"
 pv BIGINT SUM DEFAULT "0" COMMENT "total page views"
)
DISTRIBUTED BY HASH(site_id) BUCKETS 8;
~~~

### Notes on aggregation model

The data in the aggregation table will be imported in batches several times, and each import is a new data version. There are three triggers for aggregation:

* When the data is imported, aggregation is performed before the data is spilled to disk.
* After the data is spilled to disk, asynchronous aggregation of multiple versions is performed in the background ;
* Multi-version multi-way aggregation is performed during the query.

During the query, the value columns are aggregated first and then filtered, and the filtered columns are stored as key  columns.

Refer to "Create Table Statement" for the list of aggregation functions supported by the aggregation model.

## Update model

### Scenarios for update model

Update model is tailored to the scenarios where data gets constantly updated. For example, in an e-commerce scenario, the status of an order often changes, and the number of order updates can exceed hundreds of millions per day. Using the duplicate model to delete old data and insert new data can barely meet the frequent update requirements. The update model is designed for such scenarios. Its suitable scenarios has the following characteristics:

* There is a demand for constantly updating a large amount of data.
* Real-time data analysis is required.

### Principle of update model

In the update model, the sort key satisfies the uniqueness constraint and is the primary key.

The StarRocks internally assigns a version number to each batch of imported data. There may be more than one version of the same primary key. When querying, it returns the largest (latest) version of the data.

| ID | value | _version |
| :---: | :---: | :---: |
| 1 | 100 | 1 |
| 1 | 101 | 2 |
| 2 | 100 | 3 |
| 2 | 101 | 4 |
| 2 | 102 | 5 |

As shown in the example above, `ID` is the primary key, `value` is the content, and`__version` is the internal version number. The data with `ID=1` has two import batches, version 1 and 2 respectively.  The data with `ID=2` has three import batches, version 3, 4 and 5 respectively. When querying, only the data of the latest version will return (as shown as follow):

| ID | value |
| :---: | :---: |
| 1 | 101 |
| 2 | 102 |

Through this mechanism, StarRocks can support the analysis of frequently updated data.

### How to use update model

In e-commerce scenarios, statistical analysis is often based on order status. Although the order status changes frequently, `create_time` and `order_id` do not change and therefore are often used as filter conditions in queries. Users set the `create_time` and `order_id` columns as primary keys(i.e., defined with the `UNIQUE KEY` keyword when creating a table), which meet the demand for order status updates and allow for quick filtering in queries.

The following is an example of creating a data table using the update model.

* Use `UNIQUE KEY`(`create_time`, `order_id`) as the primary key, where `create_time` and`order_id` are in the queue, and their definitions appear before the definitions of other columns;
* `order_state` and `total_price` are value columns, and their aggregation type is `REPLACE`.

~~~sql
CREATE TABLE IF NOT EXISTS detail (
 create_time DATE NOT NULL COMMENT "create time of an order",
 order_id BIGINT NOT NULL COMMENT "id of an order",
 order_state INT COMMENT "state of an order",
 total_price BIGINT COMMENT "price of an order"
)
UNIQUE KEY(create_time, order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
~~~

### Notes on update model

* When importing data, you need to complete all fields, i.e., `create_time`, `order_id`, `order_state` and `total_price` in the above example.
* When reading data in an update model, multiple versions need to be merged during the query. Given the large number of versions the query performance will be degraded. Therefore, you should reduce the frequency of importing data to an update model. The import frequency should be designed to meet the business requirements for real-time performance.
* When querying, filtering is usually performed after multiple versions are merged. To improve query performance, place the value columns that are often filtered but not modified on the primary key. During merging, all primary keys will be compared. For better performance, users should avoid defining too many primary keys. If a column is only occasionally present as a filter condition, it does not need to be placed in the primary key.
