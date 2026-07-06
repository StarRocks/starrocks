## 2.7 免回表(覆盖)查询测试设计:排序键 vs 非排序键

**目的**:验证「查询不需要回表」时,二级索引相比 ORDER BY 的表现。关键区分谓词列**是否为表的物理排序键**。

**表设计**(均 100M 行、单 tablet、BUCKETS 1):

| 表 | 物理排序 | 二级索引 |
|---|---|---|
| `orders` | 无(靠二级索引) | `idx_user_town(user_id, town)`、`idx_ts(create_ts)` |
| `orders_ob` | `ORDER BY(user_id, town)` | 无(对照) |

**两类谓词**:

- **排序键谓词**:`WHERE user_id BETWEEN ...` —— `user_id` 正是 `orders_ob` 的前导排序键(ORDER BY 的主场)。
- **非排序键谓词**:`WHERE create_ts BETWEEN ...` —— `create_ts` 不在 `orders_ob` 的排序键里(ORDER BY 无法剪枝,被迫全表扫)。

**免回表查询**:`SELECT COUNT(*)`(输出列 ⊆ 索引列,无需回表)。对比三条路径:

1. **覆盖(covering)**:完全从 `.idx` 回答,不碰 base 表。
2. **lookup**:二级索引出候选位图 → 回表扫命中行。
3. **ORDER BY**:对照表的物理有序扫描。

**覆盖路径的两项关键优化**:

- **并行切片**:`.idx` 每 base 行一条记录(`N_idx == N_base`),据此把 `.idx` 行空间按 morsel 的 base-rowid 覆盖映射成不相交切片,各 morsel 并行扫各自切片,无重复扫描。
- **无删除快路径**:rowset 无 delete vector 时,跳过读取 8 字节/行的 `__sidx_pos__` 位置列、跳过逐行解码与 DelVec 判定,`COUNT(*)` 退化为「数匹配的 `.idx` 行数」。

### 测试 SQL

**建表**(`orders` 靠二级索引、`orders_ob` 物理 ORDER BY,其余完全相同):

```sql
CREATE TABLE orders (
    order_id   VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL,
    user_id    VARCHAR(10) NOT NULL, town      VARCHAR(64) NOT NULL,
    type       VARCHAR(16) NOT NULL, amount    VARCHAR(20),
    desc_blob  VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64)
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");

-- 对照表:同 schema,唯一差异是物理排序键
CREATE TABLE orders_ob ( ... 同上 10 列 ... )
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
ORDER BY (user_id, town)
PROPERTIES ( ... 同上 ... );
```

**二级索引注册**(PoC 尚无 `CREATE INDEX` DDL,经 BE config 注册,需在导入前开启):

```bash
# 在 orders 的 tablet 上注册两个索引,并开启写入
curl -X POST "http://${CN}:8040/api/update_config?enable_secondary_index_write=true"
curl -X POST "http://${CN}:8040/api/update_config?secondary_index_defs=<tablet_id>:idx_user_town:user_id,town;<tablet_id>:idx_ts:create_ts"
```

**测试查询**(免回表 `COUNT(*)`,输出列 ⊆ 索引列):

```sql
-- 排序键谓词(= orders_ob 前导排序键)
SELECT COUNT(*) FROM orders WHERE user_id BETWEEN '12345' AND '12444';   -- 0.55%  (550K 行)
SELECT COUNT(*) FROM orders WHERE user_id BETWEEN '14000' AND '14999';   -- 5.5%   (5.5M 行)
SELECT COUNT(*) FROM orders WHERE user_id BETWEEN '10000' AND '14999';   -- 27.8%  (27.8M 行)

-- 非排序键谓词(不在 orders_ob 排序键内)
SELECT COUNT(*) FROM orders WHERE create_ts BETWEEN '2025-06-01 00:00:00' AND '2025-06-02 00:00:00'; -- 1 天 (114K)
SELECT COUNT(*) FROM orders WHERE create_ts BETWEEN '2025-06-01 00:00:00' AND '2025-06-08 00:00:00'; -- 1 周 (802K)
SELECT COUNT(*) FROM orders WHERE create_ts BETWEEN '2025-06-01 00:00:00' AND '2025-07-01 00:00:00'; -- 1 月 (3.4M)
```

**三条路径的切换**(同一条 SQL,只改配置 / 改表):

| 路径 | 查询的表 | 配置 |
|---|---|---|
| 覆盖(免回表) | `orders` | `enable_secondary_index_read=true`、`enable_secondary_index_covering=true` |
| lookup(回表) | `orders` | `enable_secondary_index_read=true`、`enable_secondary_index_covering=false` |
| ORDER BY(对照) | `orders_ob` | —(对照表无需索引) |

## 3.5 免回表(覆盖)查询结果:覆盖索引 vs ORDER BY(p50,ms)

### ① 排序键谓词(`user_id`)—— ORDER BY 主场,覆盖索引已逼近

| 选择度 | 命中行 | 覆盖索引 | lookup(回表) | ORDER BY |
|---|---|---|---|---|
| 0.55% | 550K | 59 | 73 | **46** |
| 5.5% | 5.5M | 88 | 142 | **64** |
| 27.8% | 27.8M | 148 | 305 | **116** |

`user_id` 是 ORDER BY 的前导键,物理最优;覆盖索引经并行化 + 无删除快路径后,差距收窄到约 **1.3×**(27.8% 时覆盖 148ms vs ORDER BY 116ms),并全面快于带回表的 lookup。

### ② 非排序键谓词(`create_ts`)—— 覆盖索引数量级胜出

| 范围 | 命中行 | 覆盖索引 | ORDER BY | 加速 |
|---|---|---|---|---|
| 1 天 | 114K | **37** | 228 | ~6× |
| 1 周 | 802K | **45** | 232 | ~5× |
| 1 月 | 3.4M | **76** | 239 | ~3× |

`create_ts` 不在 ORDER BY 排序键内,ORDER BY 恒定全表扫(~230ms);覆盖索引按 `create_ts` 收窄,选择度越低优势越大。

### 结论

免回表场景下,二级索引在**非排序键**谓词上比 ORDER BY 快 **3–6×**(这是它的核心价值——一张表只能物理排序一个键,但可建多个二级索引);在 ORDER BY 的本命**排序键**谓词上也已逼近到 ~1.3×。剩余差距为构造性下界:ORDER BY 做 `COUNT(*)` 几乎不读列数据(靠区间边界),覆盖索引至少仍需读一个 carrier 列。
