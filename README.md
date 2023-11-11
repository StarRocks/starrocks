
 <img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=e8355b6b-a9fc-4d4e-8ed8-b3157aa1827d" />
 <p align="center">
 <a href="https://starrocks.io/index">
    <img  width="900" src="https://cdn.starrocks.io/static/github/starrocks.png">
   </a>
</p>
<p align="center">
  <a href="https://starrocks.io/download/community">Download</a> | <a href="https://docs.starrocks.io/">Docs</a> | <a href="https://starrocks.io/blog/benchmark-test">Benchmarks</a> | <a href="https://github.com/StarRocks/demo">Demo</a>
</p>
<p align="center">

 <img src="https://img.shields.io/badge/Made%20with-JAVA%20%26%20C%2B%2B-red" alt="JAVA&C++">
    </a>
    <img src="https://img.shields.io/github/commit-activity/m/StarRocks/starrocks" alt="Commit Activities">
    </a>
   <a href="https://github.com/StarRocks/starrocks/issues">
    <img src="https://img.shields.io/github/issues-raw/StarRocks/starrocks" alt="Open Issues">
  </a>
  </a>
   <a href="https://starrocks.io/index">
    <img src="https://img.shields.io/badge/Visit%20StarRocks-Website-green" alt="Website">
  </a>
  </a>
   <a href="https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg">
    <img src="https://img.shields.io/badge/Join-Slack-ff69b4" alt="Slack">
  </a>
  </a>
   <a href="https://twitter.com/StarRocksLabs">
    <img src="https://img.shields.io/twitter/follow/StarRocksLabs?style=social" alt="Twitter">
  </a>
 </p>

<div align="center"> 

  </div>
StarRocks is the next-generation data platform designed to make data-intensive real-time analytics fast and easy. 
It delivers query speeds 5 to 10 times faster than other popular solutions. StarRocks can perform real-time analytics well while updating historical records. It can also enhance real-time analytics with historical data from data lakes easily. With StarRocks, you can get rid of the de-normalized tables and get the best performance and flexibility. <br>

Learn more 👉🏻 [Introduction to StarRocks](https://www.starrocks.io/blog/introduction_to_starrocks )

<br>
 <p align="center">
    <img src="https://cdn.starrocks.io/static/github/community.gif">
   </a>
</p>
</br>

## Features

* **🚀 Native vectorized SQL engine:** StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses, which is 5 to 10 times faster than previous systems.
* **📊 Standard SQL:** StarRocks supports ANSI SQL syntax (fully supported TPC-H and TPC-DS). It is also compatible with the MySQL protocol. Various clients and BI software can be used to access StarRocks.
* **💡 Smart query optimization:** StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.
* **⚡ Real-time update:** The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.
* **🪟 Intelligent materialized view:** The materialized view of StarRocks can be automatically updated during the data import and automatically selected when the query is executed.
* **✨ Querying data in data lakes directly**: StarRocks allows direct access to data from Apache Hive™, Apache Iceberg™, and Apache Hudi™ without importing.
* **🎛️ Resource management**: This feature allows StarRocks to limit resource consumption for queries and implement isolation and efficient use of resources among tenants in the same cluster.
* **💠 Easy to maintain**: Simple architecture makes StarRocks easy to deploy, maintain and scale out. StarRocks tunes its query plan agilely, balances the resources when the cluster is scaled in or out, and recovers the data replica under node failure automatically.



<br>
  
## Architecture Overview

 <p align="center">
    <img src="images/arch.png">
   </a>
</p>

StarRocks’s streamlined architecture is mainly composed of two modules: Frontend (FE) and Backend (BE).  The entire system eliminates single points of failure through seamless and horizontal scaling of FE and BE, as well as replication of metadata and data.

Starting from version 3.0, StarRocks supports a new shared-data architecture, which can provide better scalability and lower costs.

 <p align="center">
    <img src="images/arch-v30.png">
   </a>
</p>


<br>

## Resources

### 📚 Read the docs

| Section | Description |
|-|-|
| [Deploy](https://docs.starrocks.io/en-us/latest/quick_start/Deploy) | Learn how to run and configure StarRocks.|
| [Articles](https://github.com/StarRocks/starrocks/discussions/categories/how-tos-tutorials-best-practices-and-architecture-articles)| How-tos, Tutorials, Best Practices and Architecture Articles. |
| [Docs](https://docs.starrocks.io/en-us/latest/introduction/StarRocks_intro)| Full documentation. |
| [Blogs](https://starrocks.io/blog) | StarRocks deep dive and user stories.  |

### ❓ Get support  
[<img align="right" width="150" src="https://firstcontributions.github.io/assets/Readme/join-slack-team.png">](https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg)
-  [Slack community: ](https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg)join technical discussions, ask questions, and meet other users!
-  [YouTube channel:](https://www.youtube.com/channel/UC38wR-ogamk4naaWNQ45y7Q/featured) subscribe to the latest video tutorials and webcasts.
-  [GitHub issues:](https://github.com/StarRocks/starrocks/issues) report an issue with StarRocks.


<br>  
  
## Contributing to StarRocks

We welcome all kinds of contributions from the community, individuals and partners. We owe our success to your active involvement.

1. See [Contributing.md](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) to get started.
2. Set up StarRocks development environment:
* [IDE Setup](https://docs.starrocks.io/en-us/main/developers/development-environment/ide-setup) 
3. Understand our [GitHub workflow](https://github.com/StarRocks/community/blob/main/Contributors/guide/workflow.md) for opening a pull request; use this [PR Template](https://github.com/StarRocks/starrocks/blob/main/.github/PULL_REQUEST_TEMPLATE.md) when submitting a pull request.
4. Pick a [good first issue](https://github.com/StarRocks/starrocks/labels/good%20first%20issue) and start contributing. 

**📝 License:** StarRocks is licensed under [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

**👥 Community Membership:** Learn more about different [contributor roles](community/membership.md) in StarRocks community.
  
<br>
  
## Used By

This project is used by the following companies. Learn more about their use cases:

- [Airbnb](https://www.youtube.com/watch?v=AzDxEZuMBwM&ab_channel=StarRocks_labs)
- [Trip.com](https://starrocks.medium.com/trip-com-starrocks-efficiently-supports-high-concurrent-queries-dramatically-reduces-labor-and-1e1921dd6bf8) 
- [Zepp Health](https://starrocks.io/blog/zeppheath) 
- [Lenovo](https://starrocks.io/blog/lenovo_en) 
