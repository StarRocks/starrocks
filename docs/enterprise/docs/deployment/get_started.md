# Introduction

import TimezoneError from '../_assets/commonMarkdown/_timezone.md'

CelerData Enterprise adds visual database management and development tools to CelerData Server, powered by StarRocks. CelerData Enterprise provides the following functions to improve operations and maintenance efficiency and reduce the costs of your clusters:

- Install, deploy, scale, upgrade, and roll back clusters.
- Monitor metrics and send alerts.
- Identify and diagnose possible issues.

CelerData Enterprise also provides an easy-to-use SQL editor for managing queries, tracking TopN scans, and analyzing query execution, helping you accelerate queries and simplify operations.

## Function highlights

### Cluster lifecycle management

- Visual cluster deployment
- Online cluster scale-out/in, visual node addition and decommissioning
- One-click upgrade and rollback

### Dynamic cluster monitoring and alerting

- Provides 200+ metrics to monitor cluster performance, queries, data ingestion, and compaction (data version merge) to achieve real-time visual monitoring.
- Identifies TopN scans and load tasks.
- Visualizes data ingestion and schema changes.
- Users can customize alerts using email and webhooks. They can also track alert records.

### SQL editor and easy query analysis

- Provides a user-friendly SQL editor for users to track historical queries.
- Provides visual analysis of query execution.
- Provides a slow query list to help users quickly identify query performance bottlenecks.

## Basic concepts

Before you install CelerData Enterprise and deploy a database cluster, get familiar with the following concepts of a StarRocks cluster: 

A StarRocks cluster consists of two types of modules: core modules and system modules.

- Core modules (enclosed in the yellow box in the following figure)
  - **Frontend (FE):** is responsible for metadata management, client connection management, query planning, and query scheduling. 
  - **Backend (BE):** is responsible for data storage, query execution, compaction, and replica management.
  - **Broker**: an intermediate service between StarRocks and external HDFS/object storage services. Brokers are used for data loading and exporting. 
- System modules (modules other than the core modules)
  - **Web**: provides a graphical interface for users.
  - **Center service**: pulls and summarizes information reported by Agents, and provides query services.  
  - **Agent**: a program for information collection. It collects information such as metrics. 

For more information about StarRocks, see [StarRocks architecture](../introduction/Architecture.md)
