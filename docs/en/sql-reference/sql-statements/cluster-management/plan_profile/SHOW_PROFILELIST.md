---
displayed_sidebar: docs
---

# SHOW PROFILELIST

## Description

Lists the query profile records cached in your StarRocks cluster. For more information about query profile, see [Query Profile Overview](../../../../administration/query_profile_overview.md).

This feature is supported from v3.1 onwards.

No privilege is required to perform this operation.

## Syntax

```SQL
SHOW PROFILELIST [LIMIT n]
```

## Parameters

`LIMIT n`: lists n most recent records.

## Return value

| **Return** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| QueryId    | The ID of the query.                                         |
| StartTime  | The start time of the query.                                 |
| Time       | The latency of the query.                                    |
| State      | The status of the query, including:`Error`: The query encounters an error.`Finished`: The query is finished.`Running`: The query is running. |
| Statement  | The statement of the query.                                  |

## Examples

Example 1: Show five most recent query profile records.

```SQL
SHOW PROFILELIST LIMIT 5;
```

## Relevant SQLs

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)
