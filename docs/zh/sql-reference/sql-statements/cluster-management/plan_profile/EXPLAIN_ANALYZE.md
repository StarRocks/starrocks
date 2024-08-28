---
displayed_sidebar: docs
---

# EXPLAIN ANALYZE

## 功能

执行指定 SQL，并显示相应的 Query Profile 文件。更多信息，参考 [Query Profile 概述](../../../../administration/query_profile_overview.md)。

此功能自 v3.1 起支持。

> **注意**
>
> 该操作需要有指定表的 SELECT 或 INSERT 权限。

## 语法

```SQL
EXPLAIN ANALYZE <statement>
```

## 参数说明

`statement`：需要查询 Query Profile 的 SQL 语句。支持 [SELECT](../../table_bucket_part_index/SELECT.md) 和 [INSERT INTO](../../loading_unloading/INSERT.md)。

## 使用说明

请注意，当您模拟并分析 INSERT INTO 语句的 Query Profile 时，实际上不会导入任何数据。默认情况下，导入事务会被强制回滚，以确保在分析过程中不会对数据进行意外更改。

## 示例

示例一：模拟分析 SELECT 语句，查询返回结果会被丢弃。

![img](../../../../_assets/Profile/text_based_explain_analyze_select.jpeg)

示例二：模拟分析 INSERT INTO 语句。执行结束后，导入事务会被强制回滚，数据不会被实际导入。

![img](../../../../_assets/Profile/text_based_explain_analyze_insert.jpeg)

## 相关 SQL

- [SHOW PROFILELIST](SHOW_PROFILELIST.md)
- [ANALYZE PROFILE](./EXPLAIN_ANALYZE.md)
