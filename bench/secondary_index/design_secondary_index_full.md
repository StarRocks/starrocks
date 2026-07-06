# StarRocks 存算分离 PK 表 · 轻量级排序二级索引 · 完整设计文档

> 适用范围:shared-data(存算分离)Primary Key 表。目标:为「非主键 / 非排序键」列的查询提供加速,且不改变基表物理布局、不拖慢主键点查、可在一张表上叠加多个。

---

## 0. 目标与设计取舍

- **要解决的问题**:PK 表只能按主键/排序键单一维度物理聚集;落在其它列上的谓词只能全表扫。`ORDER BY` 只能优化一个维度,且会拖慢主键点查,不可叠加。
- **核心定位**:一个**主表之外的、按索引列有序的边车文件**,只存「索引列 + 行位置指针」,不存整行。一张表可建多个,互不影响主键与基表布局。
- **关键取舍**:
  - 选**有序 run**(可块内二分定位范围),不选 Roaring 倒排——主用例是高基数列范围查询,倒排宽范围会 OR 爆炸。
  - 选**行级精确位置**(支持覆盖/免回表),不退化为粗粒度跳数索引——这是相对 ORDER BY 的差异化价值。
  - 构建走**外部归并思路**(小批排序成 run + 后续归并),不做「整 rowset 全内存排序」——避免 OOM、可流式。

> **[配图 1 — 架构 / 定位]**
> AI prompt: "Clean flat technical comparison diagram, white background, English labels, blue/teal accent. LEFT panel titled 'ORDER BY': one large table box physically re-sorted by a single key 'user_id', note 'one physical order only; PK point query slowed'. RIGHT panel titled 'Sorted Secondary Index': one unchanged base table box 'base data (sorted by PK)', plus three small separate side-car file boxes 'index on user_id', 'index on create_ts', 'index on town', each a thin sorted strip with dashed arrows back to the base table labeled 'row-position pointer'; note 'base layout unchanged; many indexes; PK point query unaffected'. Minimal labeled rounded rectangles, no photorealism."

---

## 1. 文件格式(.sidx run 文件)

### 1.1 合成 schema

每个 run 文件是一个 **segment v2 格式**的小文件,schema:

```
[ 索引列 1, …, 索引列 K, __sidx_pos__ BIGINT ]
```

- 文件内**按索引列前缀排序**;
- 索引列在合成 schema 里标记为 key / sort_key,默认开 **page 级 zone-map + bloom filter**;
- `__sidx_pos__` = 行位置指针:

```
__sidx_pos__ = (segment_ordinal << 32) | rowid_in_segment      // int64
```

`segment_ordinal` 是该行所在 segment 在 **本 rowset 内的序号**;全局 rssid = `rowset_id + segment_ordinal`(查询时还原,用于 DelVec)。因为位置是「rowset 内绝对位置」,**一个 run 文件可以跨多个 segment**。

> **[配图 2 — 文件格式与位置编码]**
> AI prompt: "Clean flat technical diagram, white background, monospace labels, blue accent. TOP: a sorted index run file as a table 'user_id | town | __sidx_pos__ (int64)' with 3-4 rows sorted ascending by user_id. BOTTOM-LEFT: a 64-bit integer split into high 32 bits 'segment_ordinal' and low 32 bits 'rowid_in_segment'. An arrow from a __sidx_pos__ value points to a specific row inside a 'base segment' box (rows as horizontal lines). Captions: 'sorted by index column' on top, 'points to exact base row' on the pointer. Flat vector, labeled."

### 1.2 run(多文件)与文件名

一个 (rowset, 索引) 可能对应**多个 run 文件**(写入期 buffer 刷出来的,彼此之间**不保证有序**;每个 run 文件**内部**有序)。文件名与 segment 同风格,用 txn + uuid 保证唯一、不含逻辑信息:

```
<txn_id>_<uuid>.sidx
```

逻辑信息(属于哪个索引、哪个 rowset)全部由元数据组织(见 §2),不放进文件名。

- 写入期:run 数 ≈ 索引数据量 / buffer 大小(默认 100MB)→ 大 rowset 几十个;
- compaction 后:归并成 1~少数个**有序**文件(见 §4)。

### 1.3 文件级剪枝信息

因为多 run 之间无序,跨 run 必须靠**文件级**信息剪枝(随机高基数列上块内 zone-map 弱,这里靠文件级 min/max + bloom):

- 每个 run 文件记录:索引列前缀的 `min_key / max_key`、`row_count`、(可选)整文件级 bloom。

---

## 2. 元数据组织

### 2.1 rowset metadata(两级)

`RowsetMetadataPB` 内,**每个二级索引一条** `SecondaryIndexPB`,其下挂该索引的**多个 run 文件**:

```protobuf
message RowsetMetadataPB {
  ...
  repeated SecondaryIndexPB secondary_indexes = 20;   // 每个二级索引一条
}

// 一个二级索引(逻辑层):名字、列只存一份
message SecondaryIndexPB {
  optional string index_name        = 1;   // 逻辑索引名
  repeated string index_col_names   = 2;   // 有序索引列
  repeated SecondaryIndexRunPB runs = 3;   // 该索引的多个 run 文件(run 内有序,run 间无序)
}

// 一个 run 索引文件(物理层)
message SecondaryIndexRunPB {
  optional string file_name = 1;   // <txn_id>_<uuid>.sidx
  optional int64  file_size = 2;
  optional int64  row_count = 3;
  optional bytes  min_key   = 4;   // 文件级 zone-map(索引列前缀)
  optional bytes  max_key   = 5;
  // bloom filter 元信息(可选)
}
```

- **两级**:索引逻辑信息(名字、列)只存一份在 `SecondaryIndexPB`;`runs` 是其物理文件列表。读路径直接遍历索引 → 其 `runs`,无需再按名字分组。
- proto 字段保持 optional/repeated;`secondary_indexes` 占 tag 20(tag 19 已被上游 `uid` 占用)。

> **[配图 3 — 两级元数据]**
> AI prompt: "Clean flat tree/hierarchy diagram, white background, blue accent, monospace labels. Root 'RowsetMetadataPB' → 'secondary_indexes []' → two 'SecondaryIndexPB' boxes (index_name='idx_user_town', cols=[user_id,town]) and (index_name='idx_ts', cols=[create_ts]). Each branches to a 'runs []' of several 'SecondaryIndexRunPB' leaves showing 'file=<txn>_<uuid>.sidx, min_key, max_key, row_count'. Emphasize two levels: logical (name/cols) once, physical (runs) many. Vertical layout, rounded rectangles."

### 2.2 索引定义(随表 schema 下发)

二级索引定义是**表 schema 的一部分**,由 FE DDL(建表内联 `INDEX … USING SORTED`,或 `ALTER TABLE … ADD INDEX`)写入,随 `TabletSchemaPB` 下发到 BE:

```protobuf
message TabletIndexPB {              // 表级 index 定义(扩展)
  optional string index_name    = 1;
  repeated string col_names     = 2;   // 有序索引列
  optional IndexType index_type = 3;   // 新增枚举值 SORTED
  optional string comment       = 4;
}
```

- 写入路径直接从 `TabletSchema` 读取本表的 SORTED 索引定义,据此决定为哪些列建 run;
- 完全走正式 DDL,不依赖任何 BE 侧配置/后门。

---

## 3. 写入流程(load)

### 3.1 总体

导入(走或不走 load spill)都经 delta writer 写 segment。为**每个 (tablet, 索引)** 准备一个**二级索引 write buffer**(默认 100MB,可配),delta writer 把 chunk append 到 segment 的同时,把**索引列值 + 行位置**也写进 buffer。

```
DeltaWriter ─append chunk──▶ TabletWriter ──▶ segment
                  │同列同步抽取
                  ▼
          二级索引 write buffer (per tablet/index, 100MB)
                  │满即触发 flush 任务
                  ▼
        secondary index flush threadpool(异步)
                  │每个任务:对 buffer 内容按索引列排序 → 写一个 run 文件
                  ▼
              run 文件 (<txn_id>_<uuid>.sidx)
```

### 3.2 flush 与 run 生成

- buffer 写满(或 segment 滚动到一定边界)→ 把当前累积的列**移交**给一个 flush 任务,丢进**异步线程池**;原 buffer 立即清空继续接收。
- flush 任务:对这一批(≤100MB)**就地排序**(`sort_and_tie_columns`,有界内存)→ 用 segment writer 写出一个 run 文件 → 返回该 run 的 `SecondaryIndexRunPB`(含 file_name/min/max/row_count)。
- **内存有界**:任意时刻 ≈ `并发(load+compact) tablet 数 × 索引数 × 100MB ×(填充+在飞行 flush)`。
- **全局内存预算**(重要):不要用「每 buffer 死 100MB」线性叠加;用一个**全局二级索引内存预算**驱动 flush——超预算就挑最大的 buffer 先刷,峰值由全局上限封住(类比 memtable flush 的全局治理)。

### 3.3 commit(finish)

- `finish()` 时:**drain** 本 tablet 所有未完成 flush 任务,把全部 run 收集为每个索引一条 `SecondaryIndexPB`(其 `runs` 挂该索引的所有 run),写进 rowset metadata,rowset 才可见。
- 任一 flush 失败 → 整个导入失败回滚。
- 异步带来的尾延迟 = 最后一个 buffer 的 flush 时间。
- 走不走 load spill 在这套实现里**已统一**:索引抽取都在 delta writer 的 append 处发生,与 spill 与否无关。

---

## 4. Compaction 流程(二阶段:只归并索引 + LCRM 重映射)

compaction **不走 write buffer 重抽 base**;等 base 段合并完成后,进入**二级索引合并第二阶段**,只读输入 rowset 的 `.sidx` 文件,归并 + 重映射位置。

### 4.1 为什么

- 只读 `.sidx`(几百 MB),不重读 base(几 GB);
- 输入 `.sidx` 本就是有序 run → k 路归并即得**有序、合并后的输出**(consolidation,解决读放大);
- 旧 rowset 的索引位置在合并后已失效,用 **LCRM(Lake Compaction Rows Mapper)** 重映射。

### 4.2 LCRM 的方向(关键)

LCRM 文件按**输出行顺序**记录每个输出行的**来源旧位置** `(rssid<<32|rowid)` → 即 **new→old**;新位置由「第 i 条 + 输出 segment 行数」隐式还原。

二级索引合并需要 **old→new**,方向相反 → 必须先把 LCRM **倒置**成 old→new:

```
inverse[old_pos] = new_pos          // 旧位置 → 新位置
```

- 形态:按输入 rssid 分的 `new_pos[rowid]` 数组(8B/行),旧位置缺失 = 已删/已覆盖(填 TOMBSTONE)。
- 量级:≈ 输入行数 × 8B(全量 100M ≈ 800MB;增量只算被合并输入)。用 mmap / 落盘 / compaction 内存预算兜底。
- 注意:更新持久化主键索引那条路按 PK 值 + 输出序更新,**不**建这张倒置表 → 这是新增内存。

### 4.3 免费红利:删除过滤

LCRM 只含**存活行**。建出 inverse 后,旧位置**不在 inverse 里 ⟺ 该行已删** → 合并时直接丢弃该条索引。**无需单独查 DelVec**,重映射 + survivor 过滤一次完成。

### 4.4 流程

```
base 段合并完成 + LCRM 写好
  ① 顺序读 LCRM(输出序):i → (new_seg,new_rowid);填 inverse[old_pos]=new_pos
  ② k 路归并所有输入 rowset 的该索引的所有 run(按索引列有序,流式、有界内存)
  ③ 每条 (index_val, old_pos):
        inverse 命中 → 输出 (index_val, new_pos)
        未命中(已删)→ 丢弃
  ④ 写出【有序 + 已重映射 + 已合并】的新索引文件(可按目标大小切成少数几个)
```

### 4.5 触发

成本低(只读 .sidx + 一次归并 + 顺带消重放大),可在 compaction 几乎**总是**做。快门可选:`索引文件数 > segment 数` 且 `overlapped == false`(已沉淀输出才投入);失败**优雅回退**(保留未合并 run,不影响正确性)。

> **[配图 4 — Compaction 二阶段 LCRM 归并]**
> AI prompt: "Clean flat two-phase pipeline diagram, white background, blue/teal accent, English labels. PHASE 1 (top): several 'input rowset' boxes with base segments flow into a 'base merge' box, producing 'output rowset (new segments)' and a side artifact 'LCRM (new->old, output order)'. PHASE 2 (bottom): left, several 'input .sidx runs (sorted by index col, OLD positions)' feed a 'k-way merge' funnel; right, 'LCRM' goes through an 'invert' box into 'inverse: old_pos -> new_pos (TOMBSTONE = deleted)' feeding the merge; merge emits 'output .sidx (sorted, NEW positions, deleted dropped, consolidated)'. Annotate arrows 'remap old->new' and 'drop deleted rows'. Funnel/merge visual, labeled rounded rectangles."

---

## 5. 查询流程(read)

### 5.1 入口与前缀门控

```
查询谓词
 │ ① 前缀门控:谓词必须命中索引「首列」,否则跳过该索引(避免无效全文件扫)
 ▼
TabletReader::get_segment_iterators
 │ ② 遍历 rowset 的每个 SecondaryIndexPB(两级:索引 → 其 runs)
 │ ③ 文件级剪枝:run 的 [min_key,max_key]/bloom 与谓词不相交 → 跳过该 run
 ▼
 两条路:lookup(回表) / covering(免回表)
```

### 5.2 lookup 路径(主路径)

```
对每个被选中的索引:
   per_index_bitmap = 空
   for run in 该索引的所有 run(未被文件级剪枝掉的):
       谓词下推到 run 的 segment scan(zone-map/bloom 剪枝)→ 解码 __sidx_pos__
       → 每 segment 一个候选 Roaring;union 进 per_index_bitmap     ← 同索引多 run 求并
   merged = (first ? per_index_bitmap : merged ∩ per_index_bitmap)   ← 跨索引求交(多索引 AND)
→ 经 SparseRange 喂给 base scan 的 presupplied_rowid_filter,只读候选行
（DelVec / 残差谓词由既有 scan pipeline 处理;lookup 结果跨 morsel 共享缓存)
```

- 并行:base scan 仍按 morsel 的 rowid_range 切分,lookup 不影响其并行度。

### 5.3 covering 路径(免回表)

当**谓词列 ∪ 输出列**都 ⊆ 某索引列时,直接从 `.sidx` 产出,不碰 base:

```
for run in 命中 run:
   取该 run [lo,hi] 行段的输出索引列,直接产出
   需要时:绝对位置 (rowset_id+seg, rowid) → 查 DelVec 过滤
```

优化:
- **无删除快路径**:rowset 无 delete vector 时,跳过读 `__sidx_pos__` 列、跳过逐行解码与 DelVec,COUNT(*) 退化为「数命中行」。
- **并行**:
  - 单个有序文件(compaction 合并后):按 `.sidx` 行号切片分给各 morsel(`N_idx==N_base` 数值映射),并行无重复;
  - 多 run(写入期未合并):以 **run 为并行单元**分给各 morsel。

### 5.4 何时启用(cost gate)

- **第一期:显式 hint** `[_USE_SORTED_INDEX_]`。
- **第二期:CBO 自动**。估「走索引的代价(候选行 → 回表触碰的 page/segment 数)」vs「全表扫(总 page 数)」,取小;否则退回全表扫。
  - 思路同 ClickHouse 选 projection 的 `sum_marks` 比较——「实测/估算要读多少单位」取最小;
  - **差异**:我们是散点回表,代价函数要计入「散读放大」(命中行触碰多少 page,page 级而非行级),不能只看候选行数(实测交叉点 ~10% 选择度)。

> **[配图 5 — 查询流程]**
> AI prompt: "Clean flat branching flowchart, white background, blue/teal accent, English labels. 'query predicate' -> 'prefix gate (must hit leading index column)' -> 'file-level prune (min/max + bloom per run)'. Then split into TWO branches. LEFT 'Lookup (read-back)': 'scan matched runs -> decode positions' -> 'union candidate bitmaps across runs' -> 'intersect across indexes (AND)' -> 'narrow base scan'. RIGHT 'Covering (no read-back)': 'read matched runs -> emit output cols' -> 'optional DelVec filter' -> 'result (no base access)'. Bottom note 'enable via hint (P1) / CBO cost gate (P2)'. Decision diamonds for the gate, rounded rectangles for steps, two labeled columns."

---

## 6. 用户接口设计

### 6.1 建表定义

```sql
CREATE TABLE orders (
    order_id BIGINT NOT NULL, create_ts DATETIME NOT NULL, user_id BIGINT NOT NULL,
    town VARCHAR(64), amount DECIMAL(18,2),
    INDEX idx_user_town (user_id, town) USING SORTED COMMENT '...',
    INDEX idx_ts        (create_ts)     USING SORTED
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 16;
```
- 多索引;复合索引按**前导前缀**生效。

### 6.2 ALTER 增删

```sql
ALTER TABLE orders ADD  INDEX idx_ts (create_ts) USING SORTED;   -- 对存量异步重建,新写入内联
ALTER TABLE orders DROP INDEX idx_ts;
```

### 6.3 查询开启

```sql
-- 第一期:hint(表级,join 也支持)
SELECT max(amount) FROM orders [_USE_SORTED_INDEX_] WHERE user_id BETWEEN 12345 AND 13444;
-- 第二期:CBO 自动识别,无需 hint
```

---

## 7. 内存与并发模型

| 维度 | 设计 |
|---|---|
| 写入内存 | per (tablet,索引) buffer(默认 100MB)+ **全局内存预算**驱动 flush;不做整 rowset 全内存排序 |
| flush 并发 | 专用 secondary index flush threadpool,异步;commit 时 drain |
| compaction 内存 | inverse(old→new)表 ~O(输入行);mmap/落盘/预算兜底 |
| 读 reader 缓存 | 进程级 LRU 缓存已打开的 run(footer+column reader),避免重复打开 |
| 读 lookup 缓存 | 同 (.sidx, 谓词) 的候选位图跨 morsel 只算一次 |

---

## 8. 分期(Roadmap)

| 阶段 | 内容 |
|---|---|
| **第一期** | **完整 DDL**(`CREATE TABLE … INDEX … USING SORTED`、`ALTER TABLE … ADD/DROP INDEX`)+ **写入**(per-(tablet,索引) buffer、异步 flush、多 run;含 compaction 的二阶段 LCRM 归并 + old→new 重映射 + 删除过滤)+ **读取**(lookup + covering;文件级 min/max+bloom 剪枝、同索引多 run union、跨索引 AND)+ **hint 开启**(`[_USE_SORTED_INDEX_]`) |
| **第二期** | **CBO 自动选择**:按代价(回表 page 数 vs 全表扫 page 数)自动决定是否走索引、下线 hint 依赖(见 §5.4)。第一期暂不做。 |

---

## 附:关键不变量

- 位置编码 `(seg_ordinal<<32)|rowid`,全局 rssid = `rowset_id + seg_ordinal`(与 base scan `_opts.rowset_id + segment_id()` 一致),保证 DelVec / 回表正确。
- 每个 `SecondaryIndexPB` 挂多个 run;run 内有序,run 间无序(靠文件级 min/max+bloom 跨 run 剪枝),compaction 归并后收敛为有序。
- 索引是**优化非正确性**:任何阶段(构建失败/未建好/超预算)都能安全退回全表扫。
