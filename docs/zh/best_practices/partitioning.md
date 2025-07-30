---
sidebar_position: 10
---

# 分区

在 StarRocks 中，实现快速分析的第一步是设计一个与查询模式相匹配的表布局。本指南将实践经验提炼为清晰的**分区**规则，帮助您：

- 通过积极的分区裁剪**减少扫描数据量**
- 使用仅元数据操作**管理生命周期任务**（TTL、GDPR 删除、分层存储）
- 随着租户数量、数据量或保留窗口的增长**平稳扩展**
- **控制写放大**——新数据进入“热”分区；Compaction 发生在历史分区中

在建模新表或重构旧表时，请牢记这些建议——每个部分都提供目标导向的标准、设计启发和操作防护措施，以避免将来进行代价高昂的重新分区。

## 分区与分桶——不同的任务

在设计高性能的 StarRocks 表时，理解分区和分桶之间的区别是至关重要的。虽然两者都有助于管理大型数据集，但它们的用途不同：

- **分区**允许 StarRocks 在查询时通过分区裁剪跳过整个数据块，并启用仅元数据的生命周期操作，如删除旧数据或特定租户的数据。
- **分桶**则有助于将数据分布在多个 tablet 上，以并行化查询执行和均衡负载，特别是在与哈希函数结合使用时。

| 方面 | 分区 | 分桶（哈希/随机） |
| ------ | ------------ | ----------------------- |
| **主要目标** | 粗粒度的数据裁剪和生命周期控制（TTL、归档）。 | 细粒度的并行度和每个分区内的数据局部性。 |
| **计划器可见性** | 分区是 catalog 对象；FE 可以通过谓词跳过它们。 | 仅相等谓词支持桶裁剪 |
| **生命周期操作** | DROP PARTITION 是仅元数据操作——理想用于 GDPR 删除、每月滚动。 | 桶不能被删除；它们仅通过 `ALTER TABLE … MODIFY DISTRIBUTED BY` 更改。 |
| **典型数量** | 每个表 10^2–10^4（天、周、租户）。 | 每个分区 10–120；StarRocks `BUCKETS xxx` 调整此值。 |
| **倾斜处理** | 合并或拆分分区；考虑复合/混合方案。 | 提高桶数量，哈希复合键，隔离“大户”，或使用随机分桶 |
| **警示信号** | >100 k 分区可能会对 FE 引入显著的内存占用 | >200 k tablet 每个 BE；超过 10 GB 的 tablet 可能遇到 compaction 问题。 |

## 什么时候应该分区？

| 表模型 | 分区？ | 典型键 |
| ---------- | ---------- | ----------- |
| 事实/事件流 | 是 | `date_trunc('day', event_time)` |
| 大型维度（数十亿行） | 有时 | 时间或业务键变更日期 |
| 小型维度/查找 | 否 | 依赖哈希分布 |

## 选择分区键

1. **时间优先默认**——如果 80% 的查询包含时间过滤器，则以 `date_trunc('day', dt)` 开始。
2. **租户隔离**——当需要按租户管理数据时，将 `tenant_id` 添加到键中。
3. **保留对齐**——将计划清除的列放入键中。
4. **复合键**：`PARTITION BY tenant_id, date_trunc('day', dt)` 可以完美裁剪，但会创建 `#tenants × #days` 个分区。保持总数低于 ≈ 100 k，否则 FE 内存和 BE compaction 会受到影响。

## 选择粒度

`PARTITION BY date_trunc('day', dt)` 的粒度应根据使用场景进行调整。您可以使用“小时”、“天”或“月”等。参见 [`date_trunc`](../sql-reference/sql-functions/date-time-functions/date_trunc.md)
| 粒度 | 适用场景 | 优点 | 缺点 |
| ----------- | -------- | ---- | ---- |
| 每日（默认） | 大多数 BI 和报告 | 少量分区（365/年）；简单的 TTL | 对“最近 3 小时”查询不够精确 |
| 每小时 | 每天 > 2 × tablet；物联网突发 | 热点隔离；24 分区/天 | 每年 8 700 分区 |
| 每周/每月 | 历史归档 | 元数据小；合并容易 | 粗粒度裁剪 |

- **经验法则**：保持每个分区 ≤ 100 GB 和每个分区 ≤ 20 k tablet（跨副本）。
- **混合粒度**：从 3.4 版本开始，StarRocks 支持通过将历史分区合并为更粗粒度来实现混合粒度。

## 示例方案

### 点击流事实（单租户）

```sql
CREATE TABLE click_stream (
  user_id BIGINT,
  event_time DATETIME,
  url STRING,
  ...
)
DUPLICATE KEY(user_id, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS xxx;
```

### SaaS 指标（多租户，模式 A）

推荐用于大多数 SaaS 工作负载。在时间上裁剪，保持租户行共置。

```sql
CREATE TABLE metrics (
  tenant_id INT,
  dt DATETIME,
  metric_name STRING,
  v DOUBLE
)
PRIMARY KEY(tenant_id, dt, metric_name)
PARTITION BY date_trunc('DAY', dt)
DISTRIBUTED BY HASH(tenant_id) BUCKETS xxx;
```

### 大租户复合（模式 B）

当需要特定租户的 DML/DDL 或存在大规模租户时，需注意潜在的分区爆炸。

```sql
CREATE TABLE activity (
  tenant_id INT,
  dt DATETIME,
  id BIGINT,
  ....
)
DUPLICATE KEY(dt, id)
PARTITION BY tenant_id, date_trunc('MONTH', dt)
DISTRIBUTED BY HASH(id) BUCKETS xxx;
```