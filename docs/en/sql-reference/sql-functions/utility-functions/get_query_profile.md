---
displayed_sidebar: "English"
---

# get_query_profile

## Description

Obtains the profile of a query by using its `query_id`. This function returns empty if the `query_id` does not exist or is incorrect.

To use this function, you must enable the profiling feature, that is, set the session variable `enable_profile` to `true` (`set enable_profile = true;`). If this feature is not enabled, an empty profile is returned.

This function is supported from v3.0.

## Syntax

```Haskell
get_query_profile(x)
```

## Parameters

`x`: the query_id string. The supported data type is VARCHAR.

## Return value

The query profile contains the following fields. For more information about the query profile, see [Query Profile](../../../administration/query_profile.md).

```SQL
Query:
  Summary:
  Planner:
  Execution Profile 7de16a85-761c-11ed-917d-00163e14d435:
    Fragment 0:
      Pipeline (id=2):
        EXCHANGE_SINK (plan_node_id=18):
        LOCAL_MERGE_SOURCE (plan_node_id=17):
      Pipeline (id=1):
        LOCAL_SORT_SINK (plan_node_id=17):
        AGGREGATE_BLOCKING_SOURCE (plan_node_id=16):
      Pipeline (id=0):
        AGGREGATE_BLOCKING_SINK (plan_node_id=16):
        EXCHANGE_SOURCE (plan_node_id=15):
    Fragment 1:
       ...
    Fragment 2:
       ...
```

## Examples

```sql
-- Enable the profiling feature.
set enable_profile = true;

-- Run a simple query.
select 1;

-- Get the query_id of the query.
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| bd3335ce-8dde-11ee-92e4-3269eb8da7d1 |
+--------------------------------------+

-- Obtain the query profile.
select get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b');

-- Use the regexp_extract function to obtain the QueryPeakMemoryUsage in the profile that matches the specified pattern.
select regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9\.]* [KMGB]*', 0);
+-----------------------------------------------------------------------------------------------------------------------+
| regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9.]* [KMGB]*', 0) |
+-----------------------------------------------------------------------------------------------------------------------+
| QueryPeakMemoryUsage: 3.828 KB                                                                                        |
+-----------------------------------------------------------------------------------------------------------------------+
```

## Related functions

- [last_query_id](./last_query_id.md)
- [regexp_extract](../like-predicate-functions/regexp_extract.md)
