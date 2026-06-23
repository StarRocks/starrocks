# 设计:分块局部细粒度二级索引(流式构建)

## 1. 背景与问题

当前 `.idx`:**每个 rowset 一个全局有序文件**,schema `[索引列…, __sidx_pos__(绝对 (seg<<32)|rowid)]`,在 `finalize()` 时对整个 rowset **全量在内存里排序一次**。

两个硬伤:
- **内存**:O(整个 rowset 的索引列 + 8B/行位置)一次性驻留 + 排序 → 大 rowset OOM(实测 581MB .idx 撞 512MB 上限);
- **效率**:排序是写入收尾的一个大串行步骤,无法与写入重叠;compaction 重建同样全量排序。

根因不是"要存行位置"(行位置写入时即知),而是选了**全局有序 + 绝对位置**的布局。

对齐 ClickHouse 全文索引的做法(`MergeTreeIndexText.h:29/37-39`:granule 内局部排序 + granule 局部行位置 + 逐 granule 流式构建),把布局改为**分块局部有序 + 局部位置**:精度不丢,全局排序消失。

## 2. 核心思想

把"rowset 全局排序"切成"**每个 block 局部排序**",block 头记 `(seg_id, first_rowid)` 用于还原绝对位置:

```
绝对位置 (seg_id, rowid_in_segment) = (block.seg_id, block.first_rowid + 局部偏移)
```

- 构建:每攒满一个 block 就**局部排序后立刻落盘**,内存只需一个 block,无全局排序、可流式;
- 查询:用 block 级 min/max 先剪枝,再在命中 block 内二分该范围,合并各 block 结果(k = 命中 block 数)。

> 块内仍用**有序 run**(不是 Roaring 倒排):我们的主用例是高基数列范围查询(`user_id BETWEEN`),有序 run 可块内二分定位范围;Roaring 对宽范围高基数会 OR 爆炸(见报告 §3.7 bitmap 实测)。

## 3. block 的定义与粒度

- **一个 block 不跨 segment**(seg_id 必须唯一,才能用 block 头还原绝对位置)。
- block 大小 = `min(block_max_rows, 该 segment 剩余行数)`,`block_max_rows` 可配,默认建议 **128K–256K 行**(或按字节,如 64MB 索引列数据)。
  - 小 → 构建内存更省、并行更细、block 级剪枝更准,但 block 多、查询端合并多;
  - 大 → block 少,但块内排序内存上升。
- 与现有边界对齐:collector 写入时已知 `(seg_id, base_rowid)`,segment 滚动即强制收尾当前 block。

## 4. 文件格式(rowset 级 .idx)

```
.idx 文件
├── Block 0 body   (块内:按索引列排序的 [索引列…, local_rowid(u32)])
├── Block 1 body
├── ……
├── Block N body
└── Block Directory(尾部目录,常驻可缓存)
      per block:
        seg_id        u32
        first_rowid   u32     // 该 block 首行在 segment 内的 rowid
        num_rows      u32
        min_key/max_key       // 索引列前缀的 min/max,块级 zone-map 剪枝
        body_offset / body_size
        (可选) bloom filter offset
```

- block body 可复用 segment-v2 的列式编码(zone-map/bloom/压缩照旧),也可后续换更轻的自定义块格式;
- `local_rowid` 存 **segment 内偏移(u32)**,比原来的 8B 绝对位置小一半、更可压缩,seg_id 不再逐行冗余存。
- 元信息 proto 改为 `repeated BlockMetaPB blocks`(对应待办 #21 多分片格式)。

## 5. 写入 / 构建路径(流式)

collector 维护「每个索引一个**当前 block 缓冲**」:

```text
add_chunk(chunk, seg_id, base_rowid):
    for 索引列值, r in chunk:
        if 当前 block 为空: block.seg_id=seg_id; block.first_rowid=base_rowid+r
        if seg_id != block.seg_id  或  block.rows == block_max_rows:
            flush_block()                     // 见下
        block.append(索引列值, local_rowid = (base_rowid+r) - block.first_rowid)

flush_block():
    sort_and_tie_columns(block 的索引列)      // 只排一个 block(有界内存)
    按 perm 写 block body(索引列 + local_rowid)
    记录 directory 条目(seg_id, first_rowid, num_rows, min/max, offset)
    reset block

finalize():
    flush 最后一个未满 block
    写 Block Directory + footer
```

- **内存 = O(block_max_rows)**,与 rowset 大小无关;
- 排序与写入重叠(边写边出 block),没有收尾大排序;
- 天然适配 **load spill / compaction**:每个 spilled run / 合并段按同一路径出 block,彻底绕开"spill 路径漏建索引"和"compaction 全量排序 OOM"两个坑。

## 6. 查询路径

### 6.1 Lookup(候选位图 → 收窄 base scan)

```text
对谓词区间 [lo, hi](命中索引前缀):
    候选 = 空 per-segment Roaring
    for block in directory:
        if [block.min_key, block.max_key] 与 [lo,hi] 不相交: skip   // 块级剪枝
        在 block 内二分定位 [lo,hi] 的行段
        for 命中行: 候选[block.seg_id].add(block.first_rowid + local_rowid)
    返回候选 → 喂 base scan 的 presupplied_rowid_filter
```

- 用单次全局二分换成「块剪枝 + 命中块内二分」;选择性高时只碰少数 block。

### 6.2 Covering(免回表)

```text
for block in 命中 block:
    取该 block [lo,hi] 行段的输出索引列,直接产出
    需 delvec 时:绝对位置 = (block.seg_id, block.first_rowid + local_rowid) → 查 delvec 过滤
    无删除时:跳过位置/delvec,直接 bulk append(沿用现有 no-delvec 快路径)
```

### 6.3 并行(替换现有的数值切片 hack)

现在覆盖扫描靠「N_idx==N_base、把 base-rowid 范围数值映射成 .idx 切片」来并行——分块后**block 本身就是并行单元**:把命中 block 集合按 morsel 平分,各 morsel 扫各自 block、无重叠、无需还原全局切片。更干净也更稳。

## 7. 与现有机制的交互

| 机制 | 影响 |
|---|---|
| **DelVec** | 不变,仍按 `(seg_id, rowid)`;block 头带 seg_id,绝对位置可还原 |
| **Compaction** | 合并段按同一流式路径出 block → **不再全量排序、消除重建 OOM** |
| **load spill** | 构建可折进 spill-merge 那一遍 → 修掉"spill 漏建索引" |
| **morsel 并行** | block 作并行单元,替换数值切片 hack |
| **多索引 AND** | 各索引候选位图按位与,不变 |

## 8. 取舍

- **得**:流式构建、内存有界(O(block))、无全局排序、无 OOM、构建可与写入/ spill/compaction 融合、block 天然并行、位置列减半。
- **失**:查询从"一次全局二分"变成"块剪枝 + 多块二分 + 合并"。对选择性查询(命中少数 block)几乎无感;对宽范围会触碰较多 block,但那本就是索引不划算、该退回全表扫的区间。
- **不再支持**:跨整个 rowset 的"天然全局有序输出";若将来要 `ORDER BY 索引列 LIMIT k` 下推,需对 block 做 k 路归并(有界,可接受)。

## 9. 调参

- `secondary_index_block_max_rows`(默认 ~256K):block 行数上限,控构建内存与查询合并的平衡;
- 块级 zone-map 默认开;bloom 对等值场景可选开。

## 10. 分期

1. **P1**:写入路径改 block 化(collector 按 block flush + 块内排序),`.idx` 加 Block Directory,proto 加 `repeated BlockMetaPB`;读路径按 block 剪枝 + 块内二分。内存/ OOM 问题即解。
2. **P2**:morsel 并行改为 block 粒度(下线数值切片 hack);compaction 重建走 block 流式。
3. **P3**:构建折进 load spill-merge 通道,去掉"导入前必须关 spill"的限制。
