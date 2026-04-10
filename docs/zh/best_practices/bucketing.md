---
sidebar_position: 30
---

# 分桶

在 StarRocks 中选择 Hash Bucketing 和 Random Bucketing 的简明指南，包括它们的机制、权衡和推荐使用场景。

---
## 快速对比

| 方面 | Hash Bucketing | Random Bucketing |
| ------ | -------------- | ---------------- |
| 示例 | `DISTRIBUTED BY HASH(id) BUCKETS 16` | `DISTRIBUTED BY RANDOM` |
| 键声明 | 需要 HASH(col1, …) | 无 – 行按轮询分配 |
| 省略时的初始桶数 | 在 CREATE 时自动选择，然后固定 | 在 CREATE 时自动选择；如果设置了 bucket_size，可以增长 |
| tablet 拆分/缩减 | 手动 ALTER … BUCKETS | 自动拆分 ⇢ 仅增长 (≥ v3.2) |
| 抗倾斜性 | 取决于键的基数 | 高 – 设计上均匀 |
| 分桶裁剪 | ✅ (过滤、连接) | 🚫 (全量 tablet 扫描) |
| Colocate Join | ✅ | 🚫 |
| 本地聚合/Shuffle Join | ✅ | 🚫 |
| 支持的表模型 | 所有 | 仅 Duplicate Key（明细）表 |

---
## Hash Bucketing

### 工作原理

通过对一个或多个列进行哈希，将行分配到 tablet。创建后，tablet 数量固定，除非手动更改。

### 要求
- 必须预先选择一个稳定、均匀、高基数的键。基数通常应是 BE 节点数量的 1000 倍，以防止哈希桶之间的数据倾斜。
- 初始选择合适的桶大小，理想情况下在 1 到 10 GB 之间。

### 优势
- 查询局部性 – 选择性过滤器和连接触达更少的 tablet。
- Colocate Join – 事实/维度表可以共享哈希键以实现高速连接。
- 可预测的布局 – 相同键的行总是放在一起。
- 本地聚合和Shuffle Join – 跨分区保持一致的哈希布局可启用本地聚合，并降低大规模连接的数据洗牌成本。

### 劣势
- 如果数据分布倾斜，容易出现热点 tablet。
- tablet 数量是静态的；扩展需要维护 DDL。
- tablet 过少会不利于数据导入、数据压缩以及查询并行度。
- tablet 过多会增加元数据开销。

### 示例：维度-事实连接和分桶裁剪

```sql
-- 事实表按 (customer_id) 哈希分桶并按天分区
CREATE TABLE sales (
  sale_id bigint,
  customer_id int,
  sale_date date,
  amount decimal(10,2)
) ENGINE = OLAP
DISTRIBUTED BY HASH(customer_id) BUCKETS 48
PARTITION BY date_trunc('DAY', sale_date)
PROPERTIES ("colocate_with" = "group1");

-- 维度表在相同键和桶数上哈希分桶，与销售表共置
CREATE TABLE customers (
  customer_id int,
  region varchar(32),
  status tinyint
) ENGINE = OLAP
DISTRIBUTED BY HASH(customer_id) BUCKETS 48
PROPERTIES ("colocate_with" = "group1");


-- StarRocks 可以进行分桶裁剪
SELECT sum(amount) 
FROM sales
WHERE customer_id = 123

-- StarRocks 可以进行本地聚合
SELECT customer_id, sum(amount) AS total_amount
FROM sales
GROUP BY customer_id
ORDER BY total_amount DESC LIMIT 100;

-- StarRocks 可以进行Colocate Join
SELECT c.region, sum(s.amount)
FROM sales s JOIN customers c USING (customer_id)
WHERE s.sale_date BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY c.region;
```

#### 从这个示例中能获得什么？

- **分桶裁剪**：`WHERE customer_id = 123` 这样的 customer_id 谓词可启用分桶裁剪，使查询仅访问一个 tablet，从而降低延迟和 CPU 开销，尤其适合点查。
- **本地聚合**：当哈希分布键是聚合键的子集时，StarRocks 可跳过洗牌聚合阶段，降低整体成本。
- **Colocate Join**：由于两个表共享桶数量和键，每个 BE 可在本地连接各自的 tablet 对，无需网络洗牌。

### 何时使用
- 稳定的表结构，具有已知的分布过滤/连接键。
- 受益于分桶裁剪的数据仓库工作负载。
- 需要特定优化，如Colocate Join/Shuffle Join/本地聚合。
- 使用聚合/主键表。

---
## Random Bucketing

### 工作原理

行按轮询分配；无需指定键。配合 `PROPERTIES ("bucket_size"="<bytes>")`，StarRocks 会在分区增大时动态拆分 tablet（v3.2+）。

### 优势

- **零设计负担**–无键、无需计算桶数。
- **抗倾斜写入**–磁盘与 BE 间压力更加均匀。
- **弹性增长**–通过 tablet 拆分，随数据或集群增长仍能保持快速导入。

### 劣势

- **无分桶裁剪**–每个查询都会扫描分区内的所有 tablet。
- **无Colocate Join**–无键布局无法利用局部性。
- 目前仅支持 **Duplicate Key（明细）表**。

### 何时使用

- 日志/事件或多租户 SaaS 表，键频繁变化或容易倾斜的场景。
- 写入密集型管道，均匀的摄取吞吐量至关重要。

---

## 操作指南

- 为随机分桶设置合适的桶大小（如 1 GiB）以启用自动拆分。
- 对于哈希分桶，监控 tablet 大小；在单个 tablet 超过 5–10 GiB 之前重分片。
