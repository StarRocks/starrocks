---
sidebar_position: 1
sidebar_label: "Overview"
keywords: ['S3 API', 'reduce cost', 'efficiency', 'efficient', 'performance']
---

# Best Practices

These best practices are written by experienced database engineers. Designing for efficiency does more than improve query speed, it decreases costs by reducing storage, CPU, and object storage (e.g., S3) API costs.

## General table design

Three guides covering:

- [Partitioning](./partitioning.md)
- [Clustering](./table_clustering.md)
- [Bucketing](./bucketing.md)

Learn about:

- The differences between partitioning and bucketing
- When to partition
- How to choose an efficient sort key
- Choosing between hash and random bucketing

## Primary key tables

The [Primary Key](./primarykey_table.md) table uses a new storage engine designed by StarRocks. Its main advantage lies in supporting real-time data updates while ensuring efficient performance for complex ad-hoc queries. In real-time business analytics, decision-making can benefit from Primary Key tables, which use the newest data to analyze results in real-time, mitigating data latency in data analysis.

Learn about:

- Choosing the type of primary key index
- Choosing the primary key
- Monitoring and managing memory use
- Tuning

