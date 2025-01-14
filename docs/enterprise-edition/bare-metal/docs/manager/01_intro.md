# Introduction

Celerdata Manager is a visualized database management and development tool. It provides the following functions to improve the O&M efficiency and cut the O&M costs of your StarRocks clusters:

- Install, deploy, scale, upgrade, and roll back clusters.
- Monitor metrics, send alerts, and diagnose and identify possible issues.

Celerdata Manager also provides an easy-to-use SQL editor to manage queries, track TopN scans, and analyze query execution, helping you accelerate queries and simplify operations.

### Function highlights

#### Cluster lifecycle management

- Visualized cluster deployment
- Online cluster scale-out/in, visualized node addition and decommissioning
- One-click upgrade and rollback

#### Dynamic cluster monitoring and alerting

- Provides 200+ metrics to monitor cluster performance, queries, data ingestion, and compaction (data version merge), achieving real-time, visualized monitoring.
- Identifies TopN scans and load tasks.
- Visualizes data ingestion and schema changes.
- Users can customize alerts using email and Webhook. They can also track alert records.

#### SQL editor and easy query analysis

- Provides a user-friendly SQL editor for users to track historical queries.
- Provides visualized analysis of query execution.
- Provides a slow query list to help users quickly identify query performance bottlenecks.

### Basic concepts

Before you install CelerData Manager and StarRocks cluster, get familiar with the following concepts of a StarRocks cluster: 

A StarRocks cluster consists of two types of modules: core modules and system modules.

- Core modules (enclosed in the yellow box in the following figure)
  - **Frontend (****FE** **):** is responsible for metadata management, client connection management, query planning, and query scheduling. 
  - **Backend (****BE****):** is responsible for data storage, query execution, compaction, and replica management.
  - **Broker**: an intermediate service between StarRocks and external HDFS/object storage services. Brokers are used for data loading and exporting. 
- System modules (modules other than the core modules)
  - **Web**: provides a visualized graphical interface for users.
  - **Center service**: pulls and summarizes information reported by Agents, and provides query services.  
  - **Agent**: a program for information collection. It collects information such as metrics. 

This content is only supported in a Feishu Docs

For more information about StarRocks, see [StarRocks architecture](https://docs.starrocks.io/en-us/latest/introduction/Architecture.).
