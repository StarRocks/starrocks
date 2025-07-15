---
displayed_sidebar: docs
keywords: ['profile', 'query']
---

# 启用 Query Profile

本主题介绍如何查看和分析 Query Profile。Query Profile 记录了查询中所有工作节点的执行信息。通过 Query Profile，您可以快速识别影响查询性能的瓶颈。

从 v3.3.0 开始，StarRocks 支持为使用 INSERT INTO FILES() 和 Broker Load 的数据导入提供 Query Profile。有关涉及的指标的详细信息，请参见 [OlapTableSink Operator](./query_profile_details.md#olaptablesink-operator)。

## 启用 Query Profile

您可以通过将变量 `enable_profile` 设置为 `true` 来启用 Query Profile：

```SQL
SET enable_profile = true;
```

### 为慢查询启用 Query Profile

不建议在生产环境中全局、长期启用 Query Profile。这是因为 Query Profile 的数据收集和处理可能会给系统带来额外负担。然而，如果您需要捕获和分析慢查询，可以仅为慢查询启用 Query Profile。这可以通过将变量 `big_query_profile_threshold` 设置为大于 `0s` 的时间来实现。例如，如果该变量设置为 `30s`，则表示只有执行时间超过 30 秒的查询才会触发 Query Profile。这确保了系统性能，同时有效监控慢查询。

```SQL
-- 30 秒
SET global big_query_profile_threshold = '30s';

-- 500 毫秒
SET global big_query_profile_threshold = '500ms';

-- 60 分钟
SET global big_query_profile_threshold = '60m';
```

### 启用运行时 Query Profile

某些查询可能需要较长时间才能执行，范围从几秒到几小时。在查询完成之前，通常很难确定查询是否仍在进行中或系统是否已崩溃。为了解决这个问题，StarRocks 在 v3.1 及之后引入了运行时 Query Profile 功能。此功能允许您在查询执行期间以固定时间间隔收集和报告 Query Profile 数据。这样，您可以实时了解查询的执行进度和潜在瓶颈，而无需等待查询完成。通过这种方式，您可以更有效地监控和优化查询过程。

当启用 Query Profile 时，此功能会自动激活，默认报告间隔为 10 秒。您可以通过修改变量 `runtime_profile_report_interval` 来调整间隔：

```SQL
SET runtime_profile_report_interval = 30;
```

运行时 Query Profile 的格式和内容与常规 Query Profile 相同。您可以像分析常规 Query Profile 一样分析运行时 Query Profile，以了解集群中运行的查询的性能指标。

### 配置 Query Profile 行为

配置设置为会话变量或 FE 动态配置项。

#### 会话变量

- **配置项**:  enable_profile 
- **有效值**:  true/false 
- **默认值**:  false 
- **描述**:  是否启用 Query Profile。`true` 表示启用此功能。

#### 会话变量

- **配置项**:  pipeline_profile_level 
- **有效值**:  1/2 
- **默认值**:  1 
- **描述**:  设置 Query Profile 的级别。`1` 表示合并 Query Profile 的指标；`2` 表示保留 Query Profile 的原始结构。如果此项设置为 `2`，则所有可视化分析工具将不再适用，因此通常不建议更改此值。

#### 会话变量

- **配置项**:  runtime_profile_report_interval 
- **有效值**:  正整数 
- **默认值**:  10 
- **描述**:  运行时 Query Profile 的报告间隔。单位：秒。

#### 会话变量

- **配置项**:  big_query_profile_threshold 
- **有效值**:  字符串 
- **默认值**:  `0s` 
- **描述**:  如果大查询的执行时间超过此值，则自动为该查询启用 Query Profile。将此项设置为 `0s` 表示禁用此功能。其值可以由一个整数加上一个单位表示，其中单位可以是 `ms`、`s`、`m`。

#### FE 动态配置项

- **配置项**:  enable_statistics_collect_profile 
- **有效值**:  true/false 
- **默认值**:  false 
- **描述**:  是否为与统计信息收集相关的查询启用 Query Profile。`true` 表示启用此功能。

### 通过 Web UI 获取 Query Profile

按照以下步骤获取 Query Profile：

1. 在浏览器中访问 `http://<fe_ip>:<fe_http_port>`。
2. 在出现的页面中，点击顶部导航中的 **queries**。
3. 在 **Finished Queries** 列表中，选择要分析的查询并点击 **Profile** 列中的链接。

![img](../../_assets/profile-1.png)

您将被重定向到所选 Query Profile 的详细页面。

![img](../../_assets/profile-2.png)

### 通过 get_query_profile 获取 Query Profile

以下示例展示了如何通过函数 get_query_profile 获取 Query Profile：

```sql
-- 启用分析功能。
set enable_profile = true;
-- 运行一个简单的查询。
select 1;
-- 获取查询的 query_id。
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| bd3335ce-8dde-11ee-92e4-3269eb8da7d1 |
+--------------------------------------+
-- 获取查询配置文件。
select get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b')\G
```

## 分析 Query Profile

Query Profile 生成的原始内容可能包含大量指标。有关这些指标的详细描述，请参见 [Query Profile 结构和详细指标](./query_profile_details.md)。

然而，大多数用户可能会发现直接分析这些原始文本并不容易。为了解决这个问题，StarRocks 提供了一种 [基于文本的 Query Profile 可视化分析](./query_profile_text_based_analysis.md) 方法。您可以使用此功能更直观地理解复杂的 Query Profile。