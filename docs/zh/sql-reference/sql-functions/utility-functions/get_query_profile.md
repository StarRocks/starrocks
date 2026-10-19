---
displayed_sidebar: docs
---

# get_query_profile



通过 `query_id` 来获取指定查询的 Profile。如果 `query_id` 不存在或不准确，返回空。

获取 Query Profile 需要开启 Profile 功能，即设置会话变量 `set enable_profile = true;`。如果未开启，该函数返回为空。

该函数从 3.0 版本开始支持。

如果将 FE 配置项 `authorization_enable_query_profile_access_check` 设置为 `true`，该函数只能获取当前用户自己执行的查询的 Profile，拥有 SYSTEM 级 OPERATE 权限的用户不受此限制。此时，若传入的 `query_id` 不是常量，或者其 Profile 未缓存在当前会话所连接的 FE 上，也需要 OPERATE 权限。若同时开启 `authorization_enable_admin_user_protection`，只有 `root` 可以传入未缓存在当前 FE 上的 `query_id`。若没有 OPERATE 权限又想查看自己的 Profile，请先执行 `select last_query_id();` 取得 ID，再将该字面量传入，用法见下方示例。

## 语法

```plaintext
get_query_profile(x)
```

## 参数说明

`x`: `query_id` 字符串。支持的数据类型是 VARCHAR。

## 返回值说明

Query Profile 一般包含以下字段，详情可参见 [查看分析 Query Profile](../../../best_practices/query_tuning/query_profile_overview.md)。

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

## 示例

```sql
-- 开启 profile 功能。
set enable_profile = true;

-- 发起一个简单查询.
select 1;

-- 获取最近一次查询的 query_id。
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| 502f3c04-8f5c-11ee-a41f-b22a2c00f66b |
+--------------------------------------+

-- 获取该查询的 Profile。由于内容较长，不在这里展示。
select get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b');

-- 搭配使用 regexp_extract 函数，从 Query Profile 中获取符合指定 Pattern 的 QueryPeakMemoryUsage。
select regexp_extract(get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b'), 'QueryPeakMemoryUsage: [0-9\.]* [KMGB]*', 0);
+-----------------------------------------------------------------------------------------------------------------------+
| regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9.]* [KMGB]*', 0) |
+-----------------------------------------------------------------------------------------------------------------------+
| QueryPeakMemoryUsage: 3.828 KB                                                                                        |
+-----------------------------------------------------------------------------------------------------------------------+
```

## 相关函数

- [last_query_id](./last_query_id.md)
- [regexp_extract](../like-predicate-functions/regexp_extract.md)
