---
displayed_sidebar: docs
description: Reports active OlapTable bookmarks with inventory, captured partition versions, and current holders.
---

# table_bookmark_summary / _partitions / _references

`information_schema.table_bookmark_summary`, `table_bookmark_partitions`, and
`table_bookmark_references` expose the active bookmark state on a StarRocks
cluster. A bookmark is an immutable record of an OlapTable's partition state
taken at a moment in time to pin partition versions for vacuum and
timestamp-based lookups.

Join the three tables on `(DB_ID, TABLE_ID, BOOKMARK_ID)`.

## table_bookmark_summary

**Bookmark inventory.** One row per active bookmark, with creation time, partition/reference counts, and at-a-glance aggregates (the 3 most recently changed partitions, the oldest and newest holder). The first table to query when you want to know what bookmarks exist on a table or in the cluster.

| Column | Type | Description |
|--------|------|-------------|
| DB_ID | BIGINT | Internal database id. |
| TABLE_ID | BIGINT | Internal table id. |
| BOOKMARK_ID | BIGINT | Bookmark id (unique within table). |
| CREATE_TIME | DATETIME | When the bookmark was taken. |
| LOGICAL_PARTITION_COUNT | BIGINT | Logical partition count captured. |
| PHYSICAL_PARTITION_COUNT | BIGINT | Physical partition count captured. |
| REFERENCE_COUNT | BIGINT | Current number of holders referencing this bookmark. |
| LATEST_CHANGED_PHYSICAL_PARTITIONS | ARRAY<STRUCT<id BIGINT, version BIGINT, time DATETIME>> | Up to 3 physical partitions with the most recent `visible_version_time`, ordered descending by time. Ties broken by largest `physical_partition_id`. Empty array if the bookmark captured no partitions; shorter than 3 when fewer partitions exist. |
| OLDEST_REFERENCE | STRUCT<id VARCHAR, time DATETIME, ttl_ms BIGINT> | Holder with the oldest current acquire time. Ties broken by lexicographically smallest holder id. `ttl_ms` is that holder's per-reference TTL in ms (`<= 0` means no TTL). |
| NEWEST_REFERENCE | STRUCT<id VARCHAR, time DATETIME, ttl_ms BIGINT> | Holder with the most recent current acquire time. Same tie-break rule. `ttl_ms` is that holder's per-reference TTL in ms (`<= 0` means no TTL). |

## table_bookmark_partitions

**Per-partition detail.** One row per (bookmark, physical_partition), showing exactly which partition versions a bookmark pins. The table to query when debugging vacuum: join with `partitions_meta` on `physical_partition_id` to spot bookmarks that hold versions older than the live table.

| Column | Type | Description |
|--------|------|-------------|
| DB_ID, TABLE_ID, BOOKMARK_ID | (mirror summary) | Join keys. |
| LOGICAL_PARTITION_ID | BIGINT | Logical partition id. |
| PHYSICAL_PARTITION_ID | BIGINT | Physical partition id. |
| VISIBLE_VERSION | BIGINT | The partition's visible version captured in this bookmark. |
| VISIBLE_VERSION_TIME | DATETIME | The wall-clock moment that visible version became visible. |
| BASE_MATERIALIZED_INDEX_META_ID | BIGINT | Base materialized index meta id at the bookmark moment. Changes on schema change / reshard. |
| BASE_MATERIALIZED_INDEX_ID | BIGINT | Base materialized index id at the bookmark moment. |

## table_bookmark_references

**Per-holder detail.** One row per (bookmark, holder), showing who is currently keeping the bookmark alive and when they acquired it. The table to query when looking up bookmarks held by a specific materialized view or other holder.

| Column | Type | Description |
|--------|------|-------------|
| DB_ID, TABLE_ID, BOOKMARK_ID | (mirror summary) | Join keys. |
| HOLDER_ID | VARCHAR | Holder identity. Materialized views encode as `mv:<dbId>-<mvId>`. |
| CREATE_TIME | DATETIME | When this holder acquired the bookmark. |
| TTL_MS | BIGINT | Per-reference time-to-live in ms set at acquire time. `<= 0` means no TTL; the background sweep never expires the reference on its own. The effective lifetime is the smaller of this and the cluster ceiling `bookmark_reference_max_ttl_ms`. |

## Query patterns

### Inventory: list all bookmarks for a table
```sql
SELECT * FROM information_schema.table_bookmark_summary
WHERE table_id = <table_id>;
```

### Vacuum debug: find partitions pinned by the oldest bookmark
```sql
SELECT p.physical_partition_id, p.visible_version
FROM information_schema.table_bookmark_partitions p
JOIN information_schema.table_bookmark_summary s
  USING (db_id, table_id, bookmark_id)
WHERE s.table_id = <table_id>
ORDER BY s.create_time ASC
LIMIT 100;
```

### MV holder lookup: find bookmarks held by an MV
```sql
SELECT * FROM information_schema.table_bookmark_references
WHERE holder_id = 'mv:1001-2003';
```

## Notes

- Rows are filtered by privilege: a user sees only bookmarks on tables they could `SELECT` from.
