---
displayed_sidebar: docs
sidebar_label: "Feature Differences"
---

import EditionSpecificFeature from '../_assets/commonMarkdown/Edition_Specific_Feature.mdx'

# Feature Differences between Shared-nothing and Shared-data Clusters

This topic lists the differences of features supported in shared-nothing and shared-data clusters.

## Real-time Analytics and Primary Key Tables

| Feature                                              | Shared-nothing | Shared-data     |
| ---------------------------------------------------- | -------------- | --------------- |
| Conditional Update                                   | v2.5+          | v3.1+           |
| Partial Update Column mode                           | v3.1           | To be supported |
| Partial Update Row mode                              | v2.3+          | v3.1+           |
| Partial update with Conditional Update               | v3.1+          | v3.4.1+         |
| Primary Key Index Persistence                        | v2.3+          | v3.2+           |
| Decoupled ORDER BY columns from Primary Keys         | v3.0+          | v3.1+           |
| Rich UPDATE and DELETE syntax for Primary Key tables | v3.0+          | v3.1+           |

## Storage Engine

| Feature                                              | Shared-nothing | Shared-data     |
| ---------------------------------------------------- | -------------- | --------------- |
| Fast Schema Evolution                                | v3.2+          | v4.0+           |
| File Bundling                                        | Not applicable | v4.0+           |
| Inverted Index                                       | v3.3+          | v4.1+           |
| Manual Compaction                                    | v3.1+          | v3.3+           |
| Row Store                                            | v3.2+          | To be supported |
| Adding/dropping STRUCT fields                        | v3.3.2+        | v3.3.5+         |
| Vector index                                         | v3.4+          | To be supported |

## Data Distribution

| Feature                                              | Shared-nothing | Shared-data     |
| ---------------------------------------------------- | -------------- | --------------- |
| Expression Partitioning                              | v3.0+          | v3.1.1+         |
| List Partitioning                                    | v3.1+          | v3.1.1+         |
| Random Bucketing                                     | v3.1+          | v3.2+           |
| Random Bucketing Optimization (with Sub-partition)   | v3.2+          | v3.2+           |
| Unified Syntax for Multi-level Expression Partition  | v3.4+          | v3.4+           |
| Optimize Table: Change Bucketing                     | v3.2+          | v3.3+           |
| Optimize Table: Change Bucketing Online              | v3.3.3+        | Not applicable  |
| Range-based Distribution                             | v4.1+          | v4.1+           |

## Query Performance

| Feature                                              | Shared-nothing | Shared-data     |
| ---------------------------------------------------- | -------------- | --------------- |
| Cross-node Data Cache Sharing                        | Not applicable | v3.5.1+         |
| Generated Column                                     | v3.1+          | v3.5+           |
| Query Cache                                          | v2.5+          | v3.4+           |
| Rollup and Synchronous Materialized view             | v1.x+          | v3.3+           |

<EditionSpecificFeature />
