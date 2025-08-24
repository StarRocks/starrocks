---
sidebar_position: 30
---

# Bucketing

A concise field guide to choosing between Hash Bucketing and Random Bucketing in StarRocks, including their mechanics, trade‑offs, and recommended use cases.

---
## Quick‑Look Comparison

| Aspect | Hash Bucketing | Random Bucketing |
| ------ | -------------- | ---------------- |
| Example | `DISTRIBUTED BY HASH(id) BUCKETS 16` | `DISTRIBUTED BY RANDOM` |
| Key declaration | Required HASH(col1, …) | None – rows assigned round‑robin |
| Initial bucket count when omitted | Auto‑chosen at CREATE, then fixed | Auto‑chosen at CREATE; can grow if bucket_size set |
| Tablet split / shrink | Manual ALTER … BUCKETS | Automatic split ⇢ growth only (≥ v3.2) |
| Skew resistance | Depends on key cardinality | High – uniform by design |
| Bucket pruning | ✅ (filters, joins) | 🚫 (full tablet scan) |
| Colocate joins | ✅ | 🚫 |
| Local aggregation / bucket-shuffle joins | ✅ | 🚫 |
| Supported table types | All | Duplicate Key tables only |

---
## Hash Bucketing

### How it Works

Rows are assigned to tablets by hashing one or more columns. Tablet count is fixed after creation unless manually altered.

### Requirements
- Must pick a stable, evenly, high‑cardinality key up front. The cardinality should typically be 1000 times more than the number of BE nodes to prevent data skew among hash buckets.
- Choose an appropriate bucket size initially, ideally ranging between 1 to 10 GB.

### Strengths
- Query locality – selective filters and joins touch fewer tablets.
- Colocate joins – fact/dim tables can share hash keys for high‑speed joins.
- Predictable layout – rows with the same key always land together.
- Local aggregation & bucket‑shuffle joins – identical hash layout across partitions enables local aggregation and reduces data shuffle costs for large join

### Weaknesses
- Vulnerable to hot tablets if data distribution skews.
- Tablet count is static; scaling requires maintenance DDL.
- Insufficient tablets can adversely affect data ingestion, data compaction, and query execution parallelism.
- Excessive use of tablets will expand the metadata footprint.

### Example: Dimension‑Fact Join and Tablet Pruning

```sql
-- Fact table partitioned and hash‑bucketed by (customer_id)
CREATE TABLE sales (
  sale_id bigint,
  customer_id int,
  sale_date date,
  amount decimal(10,2)
) ENGINE = OLAP
DISTRIBUTED BY HASH(customer_id) BUCKETS 48
PARTITION BY date_trunc('DAY', sale_date)
PROPERTIES ("colocate_with" = "group1");

-- Dimension table hash‑bucketed on the same key and bucket count colocated with the sales table
CREATE TABLE customers (
  customer_id int,
  region varchar(32),
  status tinyint
) ENGINE = OLAP
DISTRIBUTED BY HASH(customer_id) BUCKETS 48
PROPERTIES ("colocate_with" = "group1");


-- StarRocks can do tablet pruning
SELECT sum(amount) 
FROM sales
WHERE customer_id = 123

-- StarRocks can do local aggregation
SELECT customer_id, sum(amount) AS total_amount
FROM sales
GROUP BY customer_id
ORDER BY total_amount DESC LIMIT 100;

-- StarRocks can do colocate join
SELECT c.region, sum(s.amount)
FROM sales s JOIN customers c USING (customer_id)
WHERE s.sale_date BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY c.region;
```

#### What do you gain from this example?

- **Tablet pruning**: The customer_id predicate `WHERE customer_id = 123`  enables bucket pruning, allowing the query to access only a single tablet, which lowers latency & CPU cycles, especially for point-lookups.
- **Local aggregation**: when the hash distribution key is a subset of the aggregation key, StarRocks can bypass the shuffle aggregation phase, reducing the overall cost.
- **Colocated join**: because both tables share bucket number and key, each BE can join its local pair of tablets without network shuffle.

### When to Use
- Stable schemas with well‑known distribution filter/join keys.
- Data warehousing workloads that benefit from bucket pruning.
- You need some specific optimization like colocate join/bucket shuffle join/local aggregation
- You are using Aggregate/Primary Key tables.

---
## Random Bucketing

### How it Works

Rows are assigned round‑robin; no key specified. With `PROPERTIES ("bucket_size"="<bytes>")`, StarRocks dynamically splits tablets as partitions grow (v3.2+).

### Strengths

- **Zero design debt**–no keys, no bucket math.
- **Skew‑proof writes**–uniform pressure across disks & BEs.
- **Elastic growth**–tablet splits keep ingest fast as data or cluster grows.

### Weaknesses

- **No bucket pruning**–every query scans all tablets in a partition.
- **No colocated joins**–keyless layout prevents locality.
- Limited to **Duplicate Key** tables today.

### When to Use

- Log/event or multi‑tenant SaaS tables where keys change or skew.
- Write‑heavy pipelines where uniform ingest throughput is critical.

---

## Operational Guidelines

- Pick a bucket size (e.g., 1 GiB) for random bucketing to enable auto‑split.
- For hash bucketing, monitor tablet size; re‑shard before tablets exceed 5–10 GiB
