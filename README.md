# StarRocks

StarRocks is an next-gen MPP-based interactive database for all your analysius, including multi-dimensional analytics, real-time analytics  and Ad-hoc query.

## Technology

* Native vectorized SQL engine：StarRocks adopts vectorization technology to leverage the parallel computing power of CPU, including SIMD instructions and Cache Affinity. Their is a 5-10 times performance advantage over previous technologies.
* Simple architecture：StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out. Also provides high availability, reliability, fault tolerance, and scalability.
* Standard SQL：StarRocks supports Ansi SQL syntax (fully supportted TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* Smart Query Optimization: StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* Realtime update: The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* Intelligent materialized view:  The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* Convenient federated queries: StarRocks make it easy to run interactive ad-hoc analytic queries against data sources of Hive, MySQL and Elasticsearch.

## User cases

* StarRocks not only provides high concurrency & low latency point lookups, but also provides high throughput queries of ad-hoc analysis.
* StarRocks unified batch data ingestion and near real-time streaming.
* Pre-aggregations, Flat tables, star and snowflake schemas are supported and all run at enhanced speed.
* StarRocks hybridize serving and analysis requirements with an easy way to deploy, develop and use them.

## Install

Please refer [deploy](https://github.com/StarRocks/docs/blob/master/quick_start/deploy.md)

## Links

* [StarRocks official site](https://www.starrocks.com) (WIP)
* [StarRocks Documentation](https://docs.starrocks.com) (WIP)

## LICENSE

Code in this repository is provided under the Elastic License 2.0. Some portions are available under open source licenses.
Please see our FAQ.
