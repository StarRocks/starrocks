---
displayed_sidebar: docs
keywords: ['profile', 'query']
---

# Overview

A Query Profile is a detailed report that provides insights into the execution of a SQL query within StarRocks. It offers a comprehensive view of the query's performance, including the time spent on each operation, the amount of data processed, and other relevant metrics. This information is invaluable for optimizing query performance, identifying bottlenecks, and troubleshooting issues.

:::tip Why this matters
80% of real-world slow queries are solved by spotting one of three red-flag metrics. This cheat-sheet gets you there before you drown in numbers.
:::

## Quick-Start

Profile a recent query:

### 1. List recent query IDs

A query ID is needed to analyze a query profile. Use `SHOW PROFILELIST;`:

```sql
SHOW PROFILELIST;
```

:::tip
`SHOW PROFILELIST` is detailed in [Text-based Query Profile Visualized Analysis](./query_profile_text_based_analysis.md). See that page if you are getting started.
:::

### 2. Open the profile side-by-side with your SQL

Run `ANALYZE PROFILE FOR <query_id>\G` or click **Profile** in the CelerData Web UI.

### 3. Skim the “Execution Overview” banner

Examine three high‑signal metrics: **QueryExecutionWallTime** (total runtime), **QueryPeakMemoryUsagePerNode**—values above roughly 80 % of BE memory hint at spill or OOM—and the ratio **QueryCumulativeCpuTime / WallTime**. If that ratio stays below about half the CPU‑core count, the query is mostly waiting on I/O or network.

If none fire, your query is usually fine—stop here.

### 4. Drill one level deeper

Identify the operators that consume the most time or the most memory, analyze their metrics, and determine the underlying cause to pinpoint performance bottlenecks.

The "Operator Metrics" section offers numerous guidelines to aid in identifying the root cause of performance issues.

## Core Concepts

### Query Execution Flow

The comprehensive execution flow of a SQL query involves the following stages:
1. **Planning**: The query undergoes parsing, analysis, and optimization, culminating in the generation of a query plan.
2. **Scheduling**: The scheduler and coordinator work together to distribute the query plan to all participating backend nodes.
3. **Execution**: The query plan is executed using the pipeline execution engine.

![SQL Execution Flow](../_assets/Profile/execution_flow.png)

### Query Plan Structure

The StarRocks plan is hierarchical: **Fragment** is the top‑level slice of work; each fragment spawns multiple **FragmentInstances** that run on different back‑end nodes. Within an instance, a **Pipeline** strings operators together; several **PipelineDrivers** run the same pipeline concurrently on separate CPU cores; an **Operator** is the atomic step—scan, join, aggregate—that actually processes data.

![profile-3](../_assets/Profile/profile-3.png)

### Pipeline Execution Engine Concepts

The Pipeline Engine is a key component of the StarRocks execution engine. It is responsible for executing the query plan in a parallel and efficient manner. The Pipeline Engine is designed to handle complex query plans and large volumes of data, ensuring high performance and scalability.

In the pipeline engine an **Operator** implements one algorithm, a **Pipeline** is an ordered chain of operators, and multiple **PipelineDrivers** keep that chain busy in parallel. A lightweight user‑space scheduler slices time so drivers progress without blocking.

![pipeline_opeartors](../_assets/Profile/pipeline_operators.png)

### Metric Merging Strategy

By default, StarRocks merges the FragmentInstance and PipelineDriver layers to reduce profile volume, resulting in a simplified three-layer structure:
- Fragment
- Pipeline
- Operator

You can control this merging behavior through the session variable `pipeline_profile_level`:
- `1` (Default): Merged three-layer structure
- `2`: Original five-layer structure
- Other values: Treated as `1`

When profile compression is on, the engine **averages time metrics** (e.g., `OperatorTotalTime`, while `__MAX_OF_` / `__MIN_OF_` record extremes), **sums counters** such as `PullChunkNum`, and leaves **constant attributes** like `DegreeOfParallelism` unchanged.

Significant differences between MIN and MAX values often indicate data skew, particularly in aggregation and join operations.
