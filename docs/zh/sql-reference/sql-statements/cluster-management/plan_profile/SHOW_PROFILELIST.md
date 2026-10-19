---
displayed_sidebar: docs
---

# SHOW PROFILELIST

## 功能

列出 StarRocks 集群中缓存的 Query Profile 记录。更多信息，参考 [Query Profile 概述](../../../../best_practices/query_tuning/query_profile_overview.md)。

此功能自 v3.1 起支持。

默认情况下，该操作无需任何权限。如果将 FE 配置项 `authorization_enable_query_profile_access_check` 设置为 `true`，用户只能查看自己执行的查询的 Query Profile 记录；查看其他用户执行的查询的 Query Profile 记录，需要 SYSTEM 级 OPERATE 权限。有关授权操作，参见 [GRANT](../../account-management/GRANT.md)。

## 语法

```SQL
SHOW PROFILELIST [LIMIT n]
```

## 参数说明

`LIMIT n`：列出最新 n 条记录。

## 返回

| **返回**  | **说明**                                                     |
| --------- | ------------------------------------------------------------ |
| QueryId   | 查询 ID。                                                    |
| StartTime | 查询开始时间。                                               |
| Time      | 查询时长。                                                   |
| State     | 查询状态，其中包括`Error`：查询异常。`Finished`：查询执行结束。`Running`：查询正在执行。 |
| Statement | 查询对应 SQL。                                               |

## 示例

示例一：列出最新 5 条 Query Profile 记录。

```SQL
SHOW PROFILELIST LIMIT 5;
```

## 相关 SQL

- [ANALYZE PROFILE](ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](EXPLAIN_ANALYZE.md)

