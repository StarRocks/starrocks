# StarRocks

StarRocks is a next-gen sub-second MPP database for full analysis scenarios, including multi-dimensional analytics, real-time analytics and ad-hoc query.

## Technology

* Native vectorized SQL engine: StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses, which is 5 to 10 times faster than previous systems.
* Simple architecture: StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out. StarRocks also provides high availability, reliability, scalability and fault tolerance.
* Standard SQL: StarRocks supports ANSI SQL syntax (fully supported TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* Smart query optimization: StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* Realtime update: The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* Intelligent materialized view: The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* Convenient query federation: StarRocks allows direct access to data from Hive, MySQL and Elasticsearch without importing.

## User cases

* StarRocks supports not only high concurrency & low latency points queries, but also high throughput ad-hoc queries.
* StarRocks unified batch and near real-time streaming data ingestion.
* Pre-aggregations, flat tables, star and snowflake schemas are supported and all run at enhanced speed.
* StarRocks hybridizes serving and analytical processing(HSAP) in an easy way. The minimalist architectural design reduces the complexity and maintenance cost of StarRocks and increases its reliability and scalability. 

## Upstream

[Apache Doris(incubating)](https://github.com/apache/incubator-doris) is the upstream of StarRocks. We are very grateful to Apache Doris(incubating) community for contributing such an excellent OLAP database.

StarRocks was developed based on version 0.13 of Apache Doris (incubating) released in early 2020. We have adopted the framework and columnar storage engine from Apache Doris(incubating), while added a full vectorized execution engine, CBO optimizer, real-time update engine, and other important features. 

Of the approximately 700K lines of code currently in StarRocks, about 40% is identical to Apache Doris(incubating), which is still under the Apache 2.0 license, leaving 60% as additions or modification.*

We will continue to contribute to Apache Doris(incubating) and help to build the open source ecosystem in the future. 

\* Statistics from GitHub, September 2021

## Build

Because of the thirdparty dependencies, we recommend building StarRocks with the development docker image we provide.

For detailed instructions, please refer to [build](https://github.com/StarRocks/docs/blob/master/development/Build_in_docker.md).

## Install

Download the current release [here](https://www.starrocks.com/en-US/download/community).  
For detailed instructions, please refer to [deploy](https://github.com/StarRocks/docs/blob/master/quick_start/Deploy.md).

## Links

* [StarRocks official website](https://www.starrocks.com)
* [StarRocks documentation](https://docs.starrocks.com)

## LICENSE

Code in this repository is provided under the [Elastic License 2.0](https://www.elastic.co/cn/licensing/elastic-license). Some portions are available under open source licenses. Please see our [FAQ](https://www.starrocks.com/en-US/product/license-FAQ).

## Contributing to StarRocks

A big thanks for your attention to StarRocks! 
In order to accept your pull request, please follow the [CONTRIBUTING.md](CONTRIBUTING.md).
