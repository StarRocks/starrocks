# StarRocks

StarRocks is a new-generation and high-speed MPP database for nearly all data analytics scenarios. We wish to provide easy and rapid data analytics. Users can directly conduct high-speed data analytics in various scenarios without complicated data preprocessing. Query speed (especially multi-tables JOIN queries) far exceeds similar products because of our streamlined architecture, full vectorized engine, newly-designed Cost-Based Optimizer (CBO) and modern materialized views. We also support efficient real-time data analytics. 

Moreover, StarRocks provides flexible and diverse data modeling, such as flat-tables, star schema, and snowflake schema. Compatible with MySQL protocols and standard SQL syntax, StarRocks can communicate smoothly across the MySQL ecosystem, for example, MySQL clients and common BI tools. It is an integrated data analytics platform that allows for high availability and simple maintenance and doesn’t rely on any other external components.

We recommend you read the [Introduction to StarRocks](https://starrocks.medium.com/introduction-to-starrocks-7bda3474e0e7?source=friends_link&sk=1288c0abc7cde1ac3a38f8e4d865b178) first. 

## Architecture
StarRocks’s streamlined architecture is mainly composed of two modules, Frontend (FE for short) and Backend (BE for short), and doesn’t depend on any external components, which makes it easy to deploy and maintain. Meanwhile, the entire system eliminates single points of failure through seamless and horizontal scaling of FE and BE, as well as replication of meta-data and data.

![Architecture of StarRocks](https://miro.medium.com/max/1400/1*Fjk8u6a39fvegV_q2_ZRyw.png)

## Technology

* **Native vectorized SQL engine**: StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses, which is 5 to 10 times faster than previous systems.
* **Simple architecture**: StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out. StarRocks also provides high availability, reliability, scalability and fault tolerance.
* **Standard SQL**: StarRocks supports ANSI SQL syntax (fully supported TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* **Smart query optimization**: StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* **Realtime update**: The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* **Intelligent materialized view**: The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* **Convenient query federation**: StarRocks allows direct access to data from Hive, MySQL and Elasticsearch without importing.

## Use cases

StarRocks can provide satisfying performance in various data analytics scenarios, including multi-dimensional screening and analysis, real-time data analytics, ad hoc analysis. StarRocks also supports thousands of concurrent users. As a result, StarRocks is widely used by companies in business intelligence, real-time data warehouse, user profiling, dashboards, order analysis, operation, and monitoring analysis, anti-fraud, and risk control. At present, over 100 medium-sized and large enterprises in various industries have used StarRocks in their online production environment, including Airbnb, JD.com, Tencent, Trip.com and other well-known companies. There are thousands of StarRocks servers running stably in the production environment.

## Fork

StarRocks forked from Apache Doris(incubating) 0.13 in early 2020. We recreated many important parts of the database from then, including a full vectorized execution engine, a brand new CBO optimizer, a novel real-time update engine, and query federation for data lakes.

Today, there are only about 30% of the code in StarRocks is identical to Apache Doris(incubating).

## Build

Because of the thirdparty dependencies, we recommend building StarRocks with the development docker image we provide.

For detailed instructions, please refer to [build](https://github.com/StarRocks/docs/blob/main/administration/Build_in_docker.md).

## Install

Download the current release [here](https://www.starrocks.io/download/community).  
For detailed instructions, please refer to [deploy](https://github.com/StarRocks/docs/blob/master/quick_start/Deploy.md).

## Links

* [StarRocks official website](https://www.starrocks.com)
* [StarRocks documentation](https://docs.starrocks.com)
* [Introduction to StarRocks](https://starrocks.medium.com/introduction-to-starrocks-7bda3474e0e7?source=friends_link&sk=1288c0abc7cde1ac3a38f8e4d865b178)

## Community
* [StarRocks slack channel](https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg)
* [StarRocks telegram group](https://t.me/joinchat/73R83y0JOnJkMTll)

## LICENSE

Code in this repository is provided under the [Elastic License 2.0](https://www.elastic.co/cn/licensing/elastic-license). Some portions are available under [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). Please see our [FAQ](https://www.starrocks.com/en-US/product/license-FAQ).

## Contributing to StarRocks

A big thanks for your attention to StarRocks! 
In order to accept your pull request, please follow the [CONTRIBUTING.md](CONTRIBUTING.md).
