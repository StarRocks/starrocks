---
displayed_sidebar: "Chinese"
---

# get_query_profile

## 功能

通过 query_id 获取 query profile. 如果通过 query_id 获取不到 query profile 返回 NULL

## 语法

```Haskell
get_query_profile(x)
```

## 参数说明

`x`: query_id 格式字符串。 支持的数据类型是 VARCHAR。

## Return value

以文本格式返回配置文件

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
