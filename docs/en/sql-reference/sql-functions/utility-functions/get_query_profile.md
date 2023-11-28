---
displayed_sidebar: "English"
---

# get_query_profile

## Description

Get query profile by query_id. Returns NULL if profile for query_id does not exist.

## Syntax

```Haskell
get_query_profile(x)
```

## Parameters

`x`: the query_id format string. The supported data types are VARCHAR.

## Return value

Returns the profile in text format

## Examples

```sql
-- enable profile
set enable_profile = true;
-- send a query.
select 1;

-- Get the query_id of the last query.
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| bd3335ce-8dde-11ee-92e4-3269eb8da7d1 |
+--------------------------------------+

-- 
select regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9\.]* [KMGB]*', 0);
+-----------------------------------------------------------------------------------------------------------------------+
| regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9.]* [KMGB]*', 0) |
+-----------------------------------------------------------------------------------------------------------------------+
| QueryPeakMemoryUsage: 3.938 KB                                                                                        |
+-----------------------------------------------------------------------------------------------------------------------+
```
