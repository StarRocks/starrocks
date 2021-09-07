# StarRocks

StarRocks is a next-gen sub-second MPP database for full analysis senarios, including multi-dimensional analytics, real-time analytics and ad-hoc query.

## Technology

* Native vectorized SQL engine: StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses, which is 5 to 10 times faster than previous systems.
* Simple architecture: StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out. StarRocks also provides high availability, reliability, scalability and fault tolerance.
* Standard SQL: StarRocks supports Ansi SQL syntax (fully supportted TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* Smart query optimization: StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* Realtime update: The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* Intelligent materialized view: The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* Convenient query federation: StarRocks allows direct access to data from Hive, MySQL and Elasticsearch without importing.

## User cases

* StarRocks supports not only high concurrency & low latency points queries, but also high throughput ad-hoc queries.
* StarRocks unified batch and near real-time streaming data ingestion.
* Pre-aggregations, flat tables, star and snowflake schemas are supported and all run at enhanced speed.
* StarRocks hybridizes serving and analytical processing(HSAP) in a easy way. The minimalist architectural design reduces the complexity and maintenance cost of StarRocks and increases its reliability and scalability. 

## Install

Download the current release: [StarRocks-SE-1.18.2.tar.gz](http://starrocks-cn-release.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-SE-1.18.2.tar.gz?Expires=1632207424&OSSAccessKeyId=***REMOVED***&Signature=Hr96qx3N6duLzn2aYF12QhepZ3Q%3D)   
For detailed instructions, please refer to [deploy](https://github.com/StarRocks/docs/blob/master/quick_start/deploy.md).

## Links

* [StarRocks official website](https://www.dorisdb.com)
* [StarRocks documentation](https://docs.dorisdb.com)

## LICENSE

Code in this repository is provided under the [Elastic License 2.0](https://www.elastic.co/cn/licensing/elastic-license). Some portions are available under open source licenses. Please see our [FAQ](https://www.dorisdb.com/en-US/product/license-FAQ).

## Contributing to StarRocks

A big thanks for your attention to StarRocks! 
In order to accept your pull request, please follow the [CONTRIBUTING.md](CONTRIBUTING.md).
