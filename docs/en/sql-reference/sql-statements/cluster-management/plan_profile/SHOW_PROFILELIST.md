---
displayed_sidebar: docs
description: "SHOW PROFILELIST lists the query profile records cached in your StarRocks cluster."
---

# SHOW PROFILELIST

SHOW PROFILELIST lists the query profile records cached in your StarRocks cluster. For more information about query profile, see [Query Profile Overview](../../../../best_practices/query_tuning/query_profile_overview.md).

This feature is supported from v3.1 onwards.

By default, no privilege is required to perform this operation. If the FE configuration item `authorization_enable_query_profile_access_check` is set to `true`, a user can list only the profiles of the queries they ran, and listing the profiles of queries run by other users requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

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
| CustomQueryId | The client-assigned custom query ID of the query, if `custom_query_id` was set for the session. Empty otherwise. |
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
