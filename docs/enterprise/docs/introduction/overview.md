---
sidebar_position: 5
---

# CelerData Enterprise

import TimezoneError from '../_assets/commonMarkdown/_timezone.mdx'

CelerData Enterprise adds visual database management and development tools to CelerData Server, powered by StarRocks. CelerData Enterprise provides the following functionality to improve operations and maintenance efficiency and reduce the costs of your clusters:

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

## Recommended reading

Before you install CelerData Enterprise and deploy a database cluster, get familiar with the following concepts: 

- Decide on a shared-nothing or shared-data [architecture](./Architecture.md)
- Plan your [node types, size, and count](../deployment/15_plan_cluster.md)
