---
displayed_sidebar: docs
---

# 基于文本的 Query Profile 可视化分析

如何通过 MySQL 客户端获取和分析基于文本的 Query Profile。

## 使用 ANALYZE PROFILE 分析现有查询的 Profile

要分析集群中现有（历史或正在运行）查询的基于文本的 Profile，首先需要使用 [SHOW PROFILELIST](../../sql-reference/sql-statements/cluster-management/plan_profile/SHOW_PROFILELIST.md) 语句获取查询的概要。此命令列出所有已成功完成、因错误失败以及仍在运行（超过 10 秒且尚未完成）的查询。通过此语句，您可以获取后续分析所需的 Query ID。语法如下：

```SQL
SHOW PROFILELIST [LIMIT <num>];
```

示例：

```SQL
SHOW PROFILELIST;
SHOW PROFILELIST LIMIT 5;
```

输出：

```plaintext
+--------------------------------------+---------------------+-------+----------+-----------------------------------------------------------------------------------------------------------------------------------+
| QueryId                              | StartTime           | Time  | State    | Statement                                                                                                                         |
+--------------------------------------+---------------------+-------+----------+-----------------------------------------------------------------------------------------------------------------------------------+
| a40456b2-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:18 | 21ms  | Finished | SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES\n    WHERE ROUTINE_TYPE="FUNCTION" AND ROUTINE_SCHEMA = "None"               |
| a3fc4060-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:17 | 39ms  | Finished | select TABLE_NAME, COLUMN_NAME from information_schema.columns\n                                    where table_schema = 'Non ... |
| a3f7d38d-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:17 | 15ms  | Finished | select connection_id()                                                                                                            |
| a3efbd3b-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:17 | 16ms  | Finished | select connection_id()                                                                                                            |
| a26ec286-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:15 | 269ms | Error    | EXPLAIN ANALYZE  SELECT c_nation, s_nation, year(lo_orderdate) AS year , SUM(lo_revenue) AS revenue FROM lineorder_flat WHERE ...  |
+--------------------------------------+---------------------+-------+----------+-----------------------------------------------------------------------------------------------------------------------------------+
```

一旦获得 Query ID，就可以使用 [ANALYZE PROFILE](../../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md) 语句进行 Query Profile 分析。语法如下：

```SQL
ANALYZE PROFILE FROM '<Query_ID>' [, <Node_ID> [, ...] ]
```

- `Query_ID`：通过 `SHOW PROFILELIST` 语句获得的查询对应的 ID。
- `Node_ID`：Profile 节点 ID。对于指定 ID 的节点，StarRocks 返回这些节点的详细指标信息。对于未指定 ID 的节点，StarRocks 仅返回概要信息。

Profile 包括以下部分：

- Summary：Profile 的概要信息。
  - QueryID
  - 版本信息
  - 查询状态，包括 `Finished`、`Error` 和 `Running`。
  - 总查询时间。
  - 内存使用情况
  - CPU 使用率最高的前 10 个节点。
  - 内存使用率最高的前 10 个节点。
  - 与默认值不同的会话变量。
- Fragments：显示每个 Fragment 中每个节点的指标。
  - 每个节点的时间、内存使用、成本估算信息和输出行数。
  - 时间使用百分比超过 30% 的节点以红色突出显示。
  - 时间使用百分比超过 15% 且小于 30% 的节点以粉色突出显示。

示例 1：查询 Query Profile 而不指定节点 ID。

![img](../../_assets/Profile/text_based_profile_without_node_id.jpeg)

示例 2：查询 Query Profile 并指定节点 ID 为 `0`。StarRocks 返回节点 ID `0` 的所有详细指标，并突出显示高使用率的指标以便于问题识别。

![img](../../_assets/Profile/text_based_profile_with_node_id.jpeg)

此外，上述方法还支持运行时 Query Profile 的显示和分析，即为正在运行的查询生成的 Profile。当启用 Query Profile 功能时，可以使用此方法获取运行超过 10 秒的查询的 Profile。

与已完成查询的相比，正在运行查询的基于文本的 Query Profile 包含以下信息：

- Operator 状态：
  - ⏳：尚未启动的 Operator。这些 Operator 可能由于依赖关系尚未开始执行。
  - 🚀：正在运行的 Operator。
  - ✅：已完成执行的 Operator。

- 总体进度：根据 `已完成执行的 Operator 数量 / 总 Operator 数量` 计算。由于缺乏数据行的详细信息，此值可能略有失真。

- Operator 进度：根据 `已处理行数 / 总行数` 计算。如果无法计算总行数，则进度显示为 `?`。

示例：

![img](../../_assets/Profile/text_based_runtime_profile.jpeg)

## 使用 EXPLAIN ANALYZE 模拟查询进行 Profile 分析

StarRocks 提供了 [EXPLAIN ANALYZE](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN_ANALYZE.md) 语句，允许您直接模拟和分析查询的 Profile。语法如下：

```SQL
EXPLAIN ANALYZE <sql_statement>
```

执行 `EXPLAIN ANALYZE` 时，StarRocks 将默认为当前会话启用 Query Profile 功能。

目前，`EXPLAIN ANALYZE` 支持两种类型的 SQL 语句：SELECT 语句和 INSERT INTO 语句。您只能在 StarRocks 的 default catalog 内部表上模拟和分析 INSERT INTO 语句的 Query Profile。请注意，在模拟和分析 INSERT INTO 语句的 Query Profile 时，不会实际导入数据。默认情况下，导入事务将被中止，以确保在分析期间不会对数据进行意外更改。

示例 1：模拟和分析 SELECT 语句。查询结果将被丢弃。

![img](../../_assets/Profile/text_based_explain_analyze_select.jpeg)

示例 2：模拟和分析 INSERT INTO 语句。导入事务将被中止。

![img](../../_assets/Profile/text_based_explain_analyze_insert.jpeg)

## 限制

- `EXPLAIN ANALYZE INSERT INTO` 语句仅支持 default catalog 中的表。
- 为了获得更好的视觉效果，输出文本包含 ANSI 字符以提供颜色、高亮等功能。建议使用 MyCLI 客户端。对于不支持 ANSI 功能的客户端，如 MySQL 客户端，可能会有一些轻微的显示错乱。通常，它们不会影响使用。例如：

![img](../../_assets/Profile/text_based_profile_not_aligned.jpeg)