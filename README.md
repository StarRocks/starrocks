
 <p align="center">
 <a href="https://www.starrocks.com/en-US/index">
    <img  height="200" src="https://github.com/kateshaowanjou/starrocks/blob/main/logo%2Bslogan.png">
   </a>
</p>
<p align="center">
  <a href="https://www.starrocks.com/en-US/download/community">Download</a> | <a href="https://docs.starrocks.com/en-us/main/introduction/StarRocks_intro">Docs</a> | <a href="https://www.starrocks.com/en-us/blog/benchmark-test">Benchmarks</a>
</p>
<p align="center">

  <img src="https://img.shields.io/badge/Made%20with-C%2B%2B-red" alt="C++">
    <img src="https://img.shields.io/github/commit-activity/m/StarRocks/starrocks" alt="Commit Activities">
    </a>
   <a href="https://github.com/StarRocks/starrocks/issues">
    <img src="https://img.shields.io/github/issues-raw/StarRocks/starrocks" alt="Open Issues">
  </a>
  </a>
   <a href="https://www.starrocks.com/en-US/index">
    <img src="https://img.shields.io/badge/Visit%20StarRocks-Website-green" alt="Website">
  </a>
  </a>
   <a href="https://join.slack.com/t/starrocks/shared_invite/zt-192zeqlc7-Afpz4mA9g0WO3rPeDbXLrw">
    <img src="https://img.shields.io/badge/Join-Slack-ff69b4" alt="Slack">
  </a>
  </a>
   <a href="https://twitter.com/StarRocksLabs">
    <img src="https://img.shields.io/twitter/follow/StarRocksLabs?style=social" alt="Twitter">
  </a>
 </p>

<div align="center"> 

  </div>
StarRocks is a next-gen data platform for real-time data analytics. Its blazing-fast query speed is 5 to 10 times faster than previous systems. 
StarRocks can smoothly integrate into your favorite BI tools by supporting MySQL protocols and standard SQL syntax.
StarRocks can be easily deployed, maintained, and scaled out due to its simple architecture that doesn't rely on any external systems.

<br>

## Features

* **🚀 Native vectorized SQL engine:** StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses, which is 5 to 10 times faster than previous systems.
* **🏠 Simple architecture:** StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out. StarRocks also provides high availability, reliability, scalability and fault tolerance.
* **📊 Standard SQL:** StarRocks supports ANSI SQL syntax (fully supported TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* **💡 Smart query optimization:** StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* **⚡ Real-time update:** The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* **✨ Intelligent materialized view:** The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* **⏩ Convenient query federation:** StarRocks allows direct access to data from Hive, MySQL and Elasticsearch without importing.

<br>
  
## Architecture Overview

 <p align="center">
    <img src="https://camo.githubusercontent.com/b53355f4cb764125160d949b37626b2afcde57fcfda09a45fc1eebc77604a465/68747470733a2f2f6d69726f2e6d656469756d2e636f6d2f6d61782f313430302f312a466a6b38753661333966766567565f71325f5a5279772e706e67">
   </a>
</p>

StarRocks’s streamlined architecture is mainly composed of two modules：Frontend (FE) and Backend (BE).  The entire system eliminates single points of failure through seamless and horizontal scaling of FE and BE, as well as replication of metadata and data. 

<br>

## Resources

### 📚 Read the docs

| Section | Description |
|-|-|
| [Deploy](https://docs.starrocks.com/en-us/main/quick_start/Deploy) | Learn how to run and configure StarRocks.|
| [Docs](https://docs.starrocks.com/en-us/main/introduction/StarRocks_intro)| Full documentation. |
| [Blogs](https://www.starrocks.com/en-US/blog) | StarRocks deep dive and user stories.  |

### ❓ Get support  
-  [Slack community: ](https://join.slack.com/t/starrocks/shared_invite/zt-192zeqlc7-Afpz4mA9g0WO3rPeDbXLrw)join technical discussions, ask questions, and meet other users!
-  [YouTube channel:](https://www.youtube.com/channel/UC38wR-ogamk4naaWNQ45y7Q/featured) subscribe to the latest video tutorials and webcasts.
-  [GitHub issues:](https://github.com/StarRocks/starrocks/issues) report an issue with StarRocks.


<br>  
  
## Contribute to StarRocks

We welcome all kinds of contributions from the community, individuals and partners. We owe our success to your active involvement.

1. See [contributing.md](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) for ways to get started.

2. Please adhere to this project's [CODE_OF_CONDUCT.md](https://github.com/StarRocks/starrocks/blob/main/CODE_OF_CONDUCT.md).

3. We require signed contributor's license agreement (aka CLA) to accept your pull request. Complete your CLA [here](https://cla-assistant.io/StarRocks/starrocks).
4. Pick a [good first issue](https://github.com/StarRocks/starrocks/labels/good%20first%20issue) and start contributing. 

**[License]** Please note StaRocks is licensed under [Elastic License 2.0](https://github.com/StarRocks/starrocks/blob/main/LICENSE.txt), with small portions of code under [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). See [FAQ](https://www.starrocks.com/en-US/product/license-FAQ) to learn more.  
  
<br>
  
## Used By

This project is used by the following companies. Learn more about their use cases:

- [Airbnb](https://www.youtube.com/watch?v=AzDxEZuMBwM&ab_channel=StarRocks_labs)
- [Trip.com](https://starrocks.medium.com/trip-com-starrocks-efficiently-supports-high-concurrent-queries-dramatically-reduces-labor-and-1e1921dd6bf8) 
- [Zepp Health](https://www.starrocks.com/en-US/blog/zeppheath) 
- [Lenovo](https://www.starrocks.com/en-us/blog/lenovo_en) 

<br>

## Acknowledgements

StarRocks is built upon Apache Doris (incubating) 0.13 in early 2020. We have recreated many important parts of the database including a full vectorized execution engine, a brand new CBO optimizer, a novel real-time update engine, and query federation for data lakes. 

Today, there are only about 30% of the code in StarRocks is identical to Apache Doris (incubating).
