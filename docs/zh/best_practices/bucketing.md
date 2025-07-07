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
| 分区裁剪 | ✅ (过滤器, 连接) | 🚫 (完整 tablet 扫描) |
| Colocate joins | ✅ | 🚫 |
| 本地聚合/桶洗牌连接 | ✅ | 🚫 |
| 支持的表模型 | 所有 | 仅明细表 |

---
## Hash Bucketing

### 工作原理

通过对一个或多个列进行哈希，将行分配到 tablets。创建后，tablet 数量是固定的，除非手动更改。

### 要求
- 必须预先选择一个稳定、均匀、高基数的键。基数通常应是 BE 节点数量的 1000 倍，以防止哈希桶之间的数据倾斜。
- 初始选择合适的桶大小，理想情况下在 1 到 10 GB 之间。

### 优势
- 查询局部性 – 选择性过滤器和连接接触更少的 tablets。
- Colocate joins – 事实/维度表可以共享哈希键以实现高速连接。
- 可预测的布局 – 相同键的行总是放在一起。
- 本地聚合和桶洗牌连接 – 跨分区的相同哈希布局使本地聚合成为可能，并减少大连接的数据洗牌成本。

### 劣势
- 如果数据分布倾斜，容易出现热点 tablets。
- tablet 数量是静态的；扩展需要维护 DDL。
- 不足的 tablets 会对数据摄取、数据压缩和查询执行并行度产生不利影响。
- 过多使用 tablets 会扩大元数据占用。

### 示例：维度-事实连接和分区裁剪

```sql
-- 事实表按 (customer_id) 分区和哈希分桶
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


-- StarRocks 可以进行 tablet 分区裁剪
SELECT sum(amount) 
FROM sales
WHERE customer_id = 123

-- StarRocks 可以进行本地聚合
SELECT customer_id, sum(amount) AS total_amount
FROM sales
GROUP BY customer_id
ORDER BY total_amount DESC LIMIT 100;

-- StarRocks 可以进行 colocate join
SELECT c.region, sum(s.amount)
FROM sales s JOIN customers c USING (customer_id)
WHERE s.sale_date BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY c.region;
```

#### 从这个示例中你能获得什么？

- **分区裁剪**: customer_id 谓词 `WHERE customer_id = 123` 启用桶裁剪，使查询仅访问一个 tablet，从而降低延迟和 CPU 周期，特别是对于点查。
- **本地聚合**: 当哈希分布键是聚合键的子集时，StarRocks 可以跳过洗牌聚合阶段，降低整体成本。
- **Colocated join**: 因为两个表共享桶数量和键，每个 BE 可以在本地连接其 tablet 对，而无需网络洗牌。

### 何时使用
- 稳定的表结构，具有已知的分布过滤/连接键。
- 受益于分区裁剪的数据仓库工作负载。
- 需要特定优化，如 colocate join/桶洗牌连接/本地聚合。
- 使用聚合/主键表。

---
## Random Bucketing

### 工作原理

行按轮询分配；无需指定键。使用 `PROPERTIES ("bucket_size"="<bytes>")`，StarRocks 会在分区增长时动态拆分 tablets (v3.2+)。

### 优势

- **零设计负担**–无键，无桶数学。
- **抗倾斜写入**–磁盘和 BEs 上均匀的压力。
- **弹性增长**–tablet 拆分保持数据或集群增长时的快速摄取。

### 劣势

- **无分区裁剪**–每个查询扫描分区中的所有 tablets。
- **无 colocated joins**–无键布局阻止局部性。
- 目前仅限于 **明细表**。

### 何时使用

- 日志/事件或多租户 SaaS 表，键变化或倾斜。
- 写入密集型管道，均匀的摄取吞吐量至关重要。

---

## 操作指南

- 为随机分桶选择一个桶大小（例如，1 GiB）以启用自动拆分。
- 对于哈希分桶，监控 tablet 大小；在 tablets 超过 5–10 GiB 之前重新分片。