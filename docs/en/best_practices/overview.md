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

## Query Tuning

[Query tuning](./query_tuning/query_plan_intro.md) is essential for achieving high performance and reliability in StarRocks. This directory brings together practical guides, reference materials, and actionable recipes to help you analyze, diagnose, and optimize query performance at every stage—from writing SQL to interpreting execution details.

Effective query tuning in StarRocks typically follows a top-down process:

1. **Identify the Problem**
2. **Collect and Analyze Execution Information**
3. **Locate the Root Cause**
4. **Apply Tuning Strategies**
5. **Validate and Iterate**

Whether you're a DBA, developer, or data engineer, these resources will help you:
- Diagnose and resolve slow or resource-intensive queries
- Understand optimizer choices and execution details
- Apply best practices and advanced tuning strategies

Start with the [overview](./query_tuning/query_plan_intro.md), dive into the references as needed, and use the recipes and tips to solve real-world performance challenges in StarRocks. 
