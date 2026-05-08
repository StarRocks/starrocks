---
displayed_sidebar: docs
---

# get_query_dump_from_query_id

`get_query_dump_from_query_id(query_id)`
`get_query_dump_from_query_id(query_id, enable_mock)`

根据 `query_id` 返回某条已执行查询的 dump。dump 中包含重现该查询规划所需的
表结构、统计信息、session 变量等上下文，JSON 格式与
[`get_query_dump`](./get_query_dump.md) 以及 `/api/query_dump` HTTP 接口
完全一致。

该函数用于线上调试和事后回溯。它会在当前 FE 的 query detail 队列中按
`query_id` 找到记录，取出原始 SQL 以及当时的 catalog / database，再调用
dumper 重新生成 dump。

## 参数

`query_id`：查询 ID，标准 StarRocks UUID 格式
(`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)。类型：VARCHAR。

`enable_mock`：（可选）`TRUE` 时使用 mock 化的表名 / 列名替换真实名字
（便于在不暴露 schema 的前提下分享 dump）。默认 `FALSE`。类型：BOOLEAN。

## 返回值

VARCHAR。JSON 格式的 query dump。如果找不到 query、记录中的 SQL 已被脱敏，
或调用方无权读取该 query，会抛错。

## 运行前提

数据来源是 query detail 队列，因此该队列的约束同样作用于本函数。本函数
会在执行最前面对两个 FE 配置开关做前置校验，当任何一个未按预期设置时
立即给出明确报错：

- `enable_collect_query_detail_info` 必须为 `true`（默认 `false`）。
  未开启时函数立即报错 `query detail collection is disabled. Set FE
  config enable_collect_query_detail_info=true ...`，避免出现误导性的
  "not found"。
- `enable_sql_desensitize_in_log` 必须为 `false`。该开关为 `true` 时
  记录的 SQL 会被改写成 digest 形式，无法用于重放；函数会直接报错
  `SQL desensitization is enabled ...`。

通过前置校验后，仍然适用以下运行期约束：

- query 必须在调用本函数的同一个 FE 上执行。detail 是 FE 内存数据，
  不在 FE 之间同步。
- detail 还必须在 `query_detail_cache_time_nanosecond`（默认 30 秒）的
  时间窗口内，过期记录会被后台清理。
- 即使当前的 `enable_sql_desensitize_in_log = false`，如果 detail
  是在脱敏开启期间写入的（实际存储为占位符），函数会以 `original sql
  not retained` 拒绝，作为纵深防御。

## 权限

调用方的完整账号身份（`user`@`host`，如 `'alice'@'10.0.0.1'`）必须与
该 query 的原执行账号完全相同，或拥有系统级 `OPERATE` 权限。仅用户名
相同、host 不同的两个账号无法相互读取对方的 query。

## 示例

```sql
-- 用户查看自己执行过的某条 query
-- （前提：FE 上 enable_collect_query_detail_info = true）
mysql> SELECT get_query_dump_from_query_id('a1b2c3d4-e5f6-7890-abcd-ef0123456789')\G

-- 同上，启用 mock 后输出的 dump 不含真实表名
mysql> SELECT get_query_dump_from_query_id('a1b2c3d4-e5f6-7890-abcd-ef0123456789', TRUE)\G

-- 缓存窗口已过期 / 未命中时的典型报错
mysql> SELECT get_query_dump_from_query_id('00000000-0000-0000-0000-000000000000');
ERROR 1064 (HY000): Getting analyzing error. Detail message: Invalid parameter
get_query_dump_from_query_id: query_id not found in query detail queue: ...
```

## 相关函数

- [`get_query_dump`](./get_query_dump.md) —— 直接对调用方传入的 SQL 字符串
  生成 dump，不经过 query detail 队列。
