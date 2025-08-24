---
sidebar_position: 10
---

# Partitioning

Fast analytics in StarRocks begin with a table layout that matches your query patterns. This guide distills hands‑on experience into clear rules for **partitioning**, helping you:

- **Scan less data** via aggressive partition pruning
- **Manage lifecycle tasks** (TTL, GDPR deletes, tiering) with metadata‑only ops;
- **Scale smoothly** as tenant counts, data volume, or retention windows grow.
- **Controlled write amplification**–New data lands in the “hot” partition; compaction happens in historical partitions

Keep this advice close when modeling a new table or refactoring an old one—each section gives purpose‑driven criteria, design heuristics, and operational guard‑rails so you avoid costly re‑partitioning down the road.

## Partitioning vs. Bucketing–different jobs

Understanding the distinction between partitioning and bucketing is fundamental when designing performant StarRocks tables. While both help manage large datasets, they serve different purposes:

- **Partitioning** allows StarRocks to skip entire blocks of data at query time using partition pruning, and enables metadata-only lifecycle operations like dropping old or tenant-specific data.
- **Bucketing**, on the other hand, helps distribute data across tablets to parallelize query execution and balance load, especially when combined with hash functions.

| Aspect | Partitioning | Bucketing (Hash/Random) |
| ------ | ------------ | ----------------------- |
| **Primary goal** | Coarse‑grain data pruning and lifecycle control(TTL, archiving). | Fine‑grain parallelism and data locality inside each partition. |
| **Planner visibility** | Partitions are catalog objects; FE can skip them via predicates. | Only equality predicates support bucket pruning |
| **Lifecycle ops** | DROP PARTITION is metadata‑only—ideal for GDPR deletes, monthly roll‑off. | Buckets can’t be dropped; they change only with `ALTER TABLE … MODIFY DISTRIBUTED BY`. |
| **Typical count** | 10^2–10^4 per table (days, weeks, tenants). | 10–120 per partition; StarRocks `BUCKETS xxx` tunes this. |
| **Skew handling** | Merge or split partitions; consider composite/hybrid scheme. | Raise bucket count, hash on compound key, isolate “whales”, or use random bucketing |
| **Red flags** | >100 k partitions can introduce significant memory footprint for FE | >200 k tablets per BE; tablets exceeding 10 GB may encounter compaction issues. |

## When should I partition?

| Table type | Partition? | Typical key |
| ---------- | ---------- | ----------- |
| Fact / event stream | Yes | `date_trunc('day', event_time)` |
| Huge dimension (billions rows) | Sometimes | Time or business key change date |
| Small dimension / lookup | No | Rely on hash distribution |

## Choosing the partition key

1. **Time‑first default**–If 80 % of queries include a time filter, lead with `date_trunc('day', dt)`.
2. **Tenant isolation**–Add `tenant_id` into the key when you need to manage the data in tenant basis
3. **Retention alignment**–Put the column you plan to purge on into the key.
4. **Composite keys**: `PARTITION BY tenant_id, date_trunc('day', dt)` prunes perfectly but creates `#tenants × #days` partitions. Keep below ≈ 100 k total or FE memory & BE compaction suffer.

## Picking granularity

The granularity of `PARTITION BY date_trunc('day', dt)` should be adjusted based on the use case. You can use "hour," "day," or "month," etc. See [`date_trunc`](../sql-reference/sql-functions/date-time-functions/date_trunc.md)
| Granularity | Use when | Pros | Cons |
| ----------- | -------- | ---- | ---- |
| Daily (default) | Most BI & reporting | Few partitions (365/yr); simple TTL | Less precise for "last 3 h" queries |
| Hourly | > 2 × tablet per day; IoT bursts | Hot‑spot isolation; 24 partitions/day | 8 700 partitions/yr |
| Weekly / Monthly | Historical archive | Tiny metadata; merges easy | Coarser pruning |

- **Rule of thumb**: Keep each partition ≤ 100 GB and ≤ 20 k tablets/partition across replicas.
- **Mixed granularity**: Starting from version 3.4, StarRocks supports mixed granularity by merging historical partitions into coarser granularity. 

## Example recipes

### Click‑stream fact (single‑tenant)

```sql
CREATE TABLE click_stream (
  user_id BIGINT,
  event_time DATETIME,
  url STRING,
  ...
)
DUPLICATE KEY(user_id, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS xxx;
```

### SaaS metrics (multi‑tenant, pattern A)

Recommended for most SaaS workloads. Prunes on time, keeps tenant rows co‑located.

```sql
CREATE TABLE metrics (
  tenant_id INT,
  dt DATETIME,
  metric_name STRING,
  v DOUBLE
)
PRIMARY KEY(tenant_id, dt, metric_name)
PARTITION BY date_trunc('DAY', dt)
DISTRIBUTED BY HASH(tenant_id) BUCKETS xxx;
```

### Whale tenant composite (pattern B)

When tenant-specific DML/DDL is necessary or large-scale tenants are present, be cautious of potential partition explosion.

```sql
CREATE TABLE activity (
  tenant_id INT,
  dt DATETIME,
  id BIGINT,
  ....
)
DUPLICATE KEY(dt, id)
PARTITION BY tenant_id, date_trunc('MONTH', dt)
DISTRIBUTED BY HASH(id) BUCKETS xxx;
```
