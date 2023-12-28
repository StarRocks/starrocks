---
displayed_sidebar: "Chinese"
---

# SHOW PROFILELIST

## 功能

列出 StarRocks 集群中缓存的 Query Profile 记录。更多信息，参考 [Query Profile 概述](../../../administration/query_profile_overview.md)。

此功能自 v3.1 起支持。

该操作无需任何权限。

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

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)

