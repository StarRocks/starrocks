<p align="center"> Logo <p>

[![Typing SVG](https://readme-typing-svg.herokuapp.com?duration=3000&color=497d8c&lines=Real-time+Analytics+Made+Easy%EF%BC%81)](https://git.io/typing-svg)

![forthebadge](https://forthebadge.com/images/badges/made-with-c-plus-plus.svg)![forthebadge](https://forthebadge.com/images/badges/built-by-developers.svg)


[![open issues](https://img.shields.io/github/issues-raw/StarRocks/starrocks)](https://github.com/StarRocks/starrocks/issues)
[![commit activity](https://img.shields.io/github/commit-activity/m/StarRocks/starrocks)](http://www.gnu.org/licenses/agpl-3.0)
[![Discourse](https://img.shields.io/discourse/topics?server=https%3A%2F%2Fforum.starrocks.com%2F)](https://forum.starrocks.com/)

[Website](https://www.starrocks.com/EN-US/index) · [Documentation](https://docs.starrocks.com/en-us/main/introduction/StarRocks_intro)  · [Slack](https://join.slack.com/t/starrocks/shared_invite/zt-192zeqlc7-Afpz4mA9g0WO3rPeDbXLrw) · [Twitter](https://twitter.com/StarRocksLabs) · [YouTube](https://www.youtube.com/channel/UC38wR-ogamk4naaWNQ45y7Q/featured) · [Benchmarks](https://www.starrocks.com/en-us/blog/benchmark-test) \download

StarRocks is a next-gen data platform for real-time data analytics. It provides blazing fast query speed which is 5 to 10 times faster than previous systems. It also supports MySQL protocols and standard SQL syntax so it can be smoothly integrated into your favorite BI tools. StarRocks doesn't rely on external systems, so it can be easily deployed, maintained, and scaled out.


## Design Overview

![App Screenshot](https://camo.githubusercontent.com/b53355f4cb764125160d949b37626b2afcde57fcfda09a45fc1eebc77604a465/68747470733a2f2f6d69726f2e6d656469756d2e636f6d2f6d61782f313430302f312a466a6b38753661333966766567565f71325f5a5279772e706e67)
StarRocks’s streamlined architecture is mainly composed of two modules： Frontend (FE) and Backend (BE).  The entire system eliminates single points of failure through seamless and horizontal scaling of FE and BE, as well as replication of meta-data and data. 

## Features

*  **🚀 Native vectorized SQL engine:** StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses, which is 5 to 10 times faster than previous systems.
* **🏠 Simple architecture:** StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out. StarRocks also provides high availability, reliability, scalability and fault tolerance.
* **📊 Standard SQL:** StarRocks supports ANSI SQL syntax (fully supported TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* **💡 Smart query optimization:** StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* **⚡ Realtime update:** The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* **✨ Intelligent materialized view:** The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* **⏩ Convenient query federation:** StarRocks allows direct access to data from Hive, MySQL and Elasticsearch without importing.


## Documentation

[Deploy](https://docs.starrocks.com/en-us/main/quick_start/Deploy)｜ [Docs](https://docs.starrocks.com/en-us/main/introduction/StarRocks_intro)｜[FAQs](https://docs.starrocks.com/en-us/main/faq/Deploy_faq)

## Support
-   Check out the  xxx bootcamps/tutorials to get started with StarRocks.
-   Join our  [Slack community](https://join.slack.com/t/starrocks/shared_invite/zt-192zeqlc7-Afpz4mA9g0WO3rPeDbXLrw)  to chat to our engineers about your use cases, questions, and support queries; for Chinese users, you can also join our [discourse forum](https://forum.starrocks.com/).
-   Subscribe to the latest video tutorials on our  [YouTube channel](https://www.youtube.com/channel/UC38wR-ogamk4naaWNQ45y7Q/featured).

## Contribute to StarRocks

We welcome all kinds of contributions from the community, individuals and partners. We owe our success to your active involvement.

1. See [contributing.md](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) for ways to get started.

2. Please adhere to this project's [CODE_OF_CONDUCT.md](https://github.com/StarRocks/starrocks/blob/main/CODE_OF_CONDUCT.md).

3. We require signed contributor's license agreement (aka CLA) to accept your pull request. Complete your CLA [here](https://cla-assistant.io/StarRocks/starrocks).
4. Pick a [good first issue](https://github.com/StarRocks/starrocks/labels/good%20first%20issue) and start contributing. 


## Used By

This project is used by the following companies. Learn more about their use cases:

- [Airbnb](https://www.youtube.com/watch?v=AzDxEZuMBwM&ab_channel=StarRocks_labs)
- [Trip.com](https://starrocks.medium.com/trip-com-starrocks-efficiently-supports-high-concurrent-queries-dramatically-reduces-labor-and-1e1921dd6bf8) 


## Acknowledgements

StarRocks is built upon Apache Doris (incubating) 0.13 in early 2020. We have recreated many important parts of the database including a full vectorized execution engine, a brand new CBO optimizer, a novel real-time update engine, and query federation for data lakes. 

Today, there are only about 30% of the code in StarRocks is identical to Apache Doris (incubating).
