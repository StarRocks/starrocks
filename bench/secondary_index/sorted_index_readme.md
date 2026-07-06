# [Readme] Sorted secondary index for Primary Key tables (casey)

## Why

A Primary Key table physically stores its data in primary-key / sort-key (`ORDER BY`) order. A query whose predicate hits that key is fast — the scanner uses the short-key index + binary search to jump to the rows. But a query whose predicate falls on **any other column** has to scan the whole table.

`ORDER BY` only accelerates **one** dimension: you can physically sort a table by a single key, and adding a non-PK sort key even slows down primary-key point lookups (the PK gives up its physical order). Many real workloads filter on columns that are neither the primary key nor the sort key. For example, Mailchimp's table has `PRIMARY KEY(account_id, contact_id, merge_field_id)` but almost every query filters on `account_id, contact_audience_id` — a full scan today.

The sorted secondary index fills this gap: a lightweight, per-column **sorted side-car file** that accelerates filters on arbitrary columns, **without** changing the table's physical layout and **without** slowing primary-key point queries. Unlike `ORDER BY`, you can create several of them on different columns of the same table.

```sql
-- Today, this is a full table scan (user_id is neither PK nor sort key):
CREATE TABLE orders (order_id BIGINT, create_ts DATETIME, user_id BIGINT, amount DECIMAL(18,2))
PRIMARY KEY(order_id, create_ts);
SELECT max(amount) FROM orders WHERE user_id BETWEEN 12345 AND 13444;   -- slow full scan
```

## What

A sorted secondary index is a small per-rowset sorted file that stores only `[index columns…, row-position pointer]` (not the full row), ordered by the index columns, with page-level zone-map + bloom filter on those columns. Storage overhead is small (~6% per index in our tests), and it never touches the base data layout or the primary key.

Compared with `ORDER BY`: `ORDER BY` is the fastest for its single key but is limited to one dimension and penalizes PK point queries; a secondary index is an additive side file — multiple indexes per table, base layout unchanged.

### 1. Define at table creation

```sql
CREATE TABLE orders (
    order_id   BIGINT        NOT NULL,
    create_ts  DATETIME      NOT NULL,
    user_id    BIGINT        NOT NULL,
    town       VARCHAR(64),
    amount     DECIMAL(18,2),
    INDEX idx_user_town (user_id, town) USING SORTED COMMENT 'filters on user_id / (user_id, town)',
    INDEX idx_ts        (create_ts)     USING SORTED
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 16;
```

- Multiple sorted indexes per table are allowed (one per column or column group).
- A composite index `(user_id, town)` accelerates predicates on its **leading prefix**: `user_id = …`, `user_id BETWEEN …`, or `user_id = … AND town = …`.

### 2. Add / drop on an existing table

```sql
ALTER TABLE orders ADD  INDEX idx_ts (create_ts) USING SORTED;
ALTER TABLE orders DROP INDEX idx_ts;
```

`ADD INDEX` builds the index asynchronously over existing rowsets; new writes build it inline. Track progress with `SHOW ALTER TABLE COLUMN FROM <db>`.

## How (enable it on a query)

### Phase 1 — explicit hint

Opt a query into the secondary index with a table hint:

```sql
SELECT max(amount) FROM orders [_USE_SORTED_INDEX_] WHERE user_id BETWEEN 12345 AND 13444;
```

The planner checks that the predicate hits the leading column of some sorted index on the table; if so, the scanner uses the index to narrow the scan to matching rows instead of a full scan. Because it is a **table-level** hint, joins work out of the box — hint the table that is being filtered:

```sql
SELECT count(o.order_id)
FROM orders [_USE_SORTED_INDEX_] o JOIN dim d ON o.town = d.town
WHERE o.user_id = 12345;
```

When all referenced columns are inside the index (a **covering** query), the result is produced from the index file directly, with no base-table read-back:

```sql
SELECT count(*)  FROM orders [_USE_SORTED_INDEX_] WHERE user_id BETWEEN 12345 AND 13444;  -- index-only
SELECT max(town) FROM orders [_USE_SORTED_INDEX_] WHERE user_id = 12345;                  -- index-only
```

### Phase 2 — automatic via CBO (planned)

In a later release the hint becomes optional. The optimizer estimates the cost of the secondary-index path (candidate rows → pages to read back) versus a full scan (total pages) and picks the cheaper plan automatically — the same "measure the work, take the minimum" idea ClickHouse uses to decide whether to read a projection. Until that lands, **use the hint**.

## Limitation

- The index helps **selective** predicates. For low-selectivity range scans (more than ~10% of the table in our tests) a full scan can be cheaper; in phase 1 (hint-driven) only add the hint where the predicate is selective. Phase 2 CBO removes this footgun by choosing automatically.
- The predicate must hit the index's **leading** column; a predicate only on a non-leading column of a composite index does not use the index.
- Available on shared-data Primary Key tables first; `ALTER ADD INDEX` async rebuild and CBO auto-selection are delivered in stages.
