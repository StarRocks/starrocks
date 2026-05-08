---
displayed_sidebar: docs
---

# get_query_dump_from_query_id

`get_query_dump_from_query_id(query_id)`
`get_query_dump_from_query_id(query_id, enable_mock)`

Returns a dump of a previously executed query, located by its `query_id`. The
dump contains the table schemas, statistics, session variables and other
context needed to reproduce planning of the original query, in the same JSON
shape produced by [`get_query_dump`](./get_query_dump.md) and the
`/api/query_dump` HTTP endpoint.

This function is intended for debugging and post-mortem analysis. It looks up
the query in StarRocks' in-memory query detail queue on the current FE,
recovers the original SQL plus its catalog and database, and re-runs the
dumper against that SQL.

## Arguments

`query_id`: The query identifier, in the standard StarRocks UUID format
(`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). Type: VARCHAR.

`enable_mock`: (Optional) When `TRUE`, the dump uses mocked table / column
names instead of the originals (handy for sharing the dump without leaking
schema). Defaults to `FALSE`. Type: BOOLEAN.

## Return Value

VARCHAR. JSON-encoded query dump. Throws an error if the query cannot be
located, the recorded SQL was desensitized, or the caller is not allowed to
read the query.

## Operational requirements

The query detail queue is the source of truth, so the same constraints that
apply to that queue apply here. The function validates the two FE-config
gates upfront and returns a clear error when either is mis-set:

- `enable_collect_query_detail_info` must be `true` (default `false`).
  When unset, the function refuses immediately with
  `query detail collection is disabled. Set FE config
  enable_collect_query_detail_info=true ...` instead of silently producing
  a "not found" error.
- `enable_sql_desensitize_in_log` must be `false`. When set to `true` the
  recorded SQL is rewritten to a digest form and cannot be re-dumped; the
  function refuses upfront with `SQL desensitization is enabled ...`.

Additional runtime constraints, surfaced after the upfront checks pass:

- The query must have been executed on the same FE that the function is
  invoked on. Detail data is per-FE in-memory state, not synchronized.
- The detail row must still be in the cache window controlled by
  `query_detail_cache_time_nanosecond` (default 30 seconds). Older queries
  are evicted by a background sweep.
- As defense in depth, even with desensitization currently off, if the
  recorded SQL was captured while desensitization was on (so the detail
  row holds the placeholder), the function refuses with `original sql not
  retained`.

## Privilege

The caller must either match the original executor's full account identity
(`user`@`host`, e.g. `'alice'@'10.0.0.1'`) or hold system-level `OPERATE`
privilege. Username-only matches are rejected, so two distinct accounts that
share a username but differ on host cannot read each other's queries.

## Examples

```sql
-- A user looking up one of their own queries (assuming
-- enable_collect_query_detail_info = true on the FE).
mysql> SELECT get_query_dump_from_query_id('a1b2c3d4-e5f6-7890-abcd-ef0123456789')\G

-- Same, but with mocked schema for safer sharing.
mysql> SELECT get_query_dump_from_query_id('a1b2c3d4-e5f6-7890-abcd-ef0123456789', TRUE)\G

-- Typical error when the query is not in the cache window.
mysql> SELECT get_query_dump_from_query_id('00000000-0000-0000-0000-000000000000');
ERROR 1064 (HY000): Getting analyzing error. Detail message: Invalid parameter
get_query_dump_from_query_id: query_id not found in query detail queue: ...
```

## See also

- [`get_query_dump`](./get_query_dump.md) — dump a SQL string supplied directly
  by the caller, without going through the query detail queue.
