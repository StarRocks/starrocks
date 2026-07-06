# StarRocks 二级索引 PoC · 测试查询清单（Q1–Q7）

把原 §2.4 测试查询、§2.5 选择度扫描、§2.6 列类型对比、§2.7 免回表测试整合为一份统一查询清单。每条查询只说明其**目的**;选择度档位与命中行见文末附表。

## 公共前提

- 数据集:1 亿行、单 tablet、`PRIMARY KEY(order_id, create_ts)`、`gen_data.py --seed 42`、复用同一份 CSV。
- 三种表布局对照(同一查询切换执行对象/配置):
  - **原始表**(索引 OFF)= 全表扫基线;
  - **二级索引**(索引 ON,可选覆盖/回表)= `orders` + `idx_user_town(user_id,town)`、`idx_ts(create_ts)`;
  - **ORDER BY** = `orders_ob`,物理 `ORDER BY(user_id, town)`。
- 索引列基数:`user_id ∈ [1,200000]`(高基数)、`town ∈ 20 值`、`create_ts` 均匀分布于 ~872 天。

---

## Q1 — 主键点查

```sql
SELECT * FROM orders WHERE order_id = '17424219';
```
**目的**:PK 索引最优基线。验证启用二级索引后主键点查不被拖慢(对照 ORDER BY 因主键让位而变慢)。

## Q2 — 高选择性等值(回表)

```sql
SELECT MAX(amount) FROM orders WHERE user_id = '12345';     -- ~540 行 / 0.0005%
```
**目的**:二级索引强项。等值命中极少行,聚合列 `amount` 不在索引内 → 必须回表,验证「索引定位 + 回表取数」的端到端收益。

## Q3 — 选择度扫描(回表读非索引列)

```sql
SELECT MAX(amount) FROM orders WHERE user_id BETWEEN '12345' AND '12444';   -- 见附表,5 档
```
**目的**:固定查询形态、调宽 `user_id` 范围覆盖 0.0005%→27.8% 五档,观察「二级索引 / ORDER BY / 原始表」收益随选择度的变化,定位**索引相对全表扫的交叉点**(回表取散落行 vs 顺序全表扫)。

## Q4 — 全表聚合(无索引收益对照)

```sql
SELECT town, COUNT(*) FROM orders GROUP BY town;
```
**目的**:谓词不命中任何索引首列。验证**前缀 gate** 正确跳过二级索引、零额外开销(索引不应在此拖慢)。

## Q5 — 列类型对比(BIGINT,回表)

```sql
SELECT MAX(amount) FROM orders_bi WHERE user_id BETWEEN 12345 AND 13444;    -- 见附表,5 档
```
**目的**:同 Q3 形态,但表用自然类型(`order_id BIGINT` / `create_ts DATETIME` / `user_id BIGINT` / `amount DECIMAL`)。数值谓词按**相同命中行数**标定,考察「基表扫描更便宜(整数解码 + 数据小)」如何使索引交叉点提前、适用区间收窄。

## Q6 — 免回表 COUNT,谓词命中排序键

```sql
SELECT COUNT(*) FROM orders WHERE user_id BETWEEN '12345' AND '12444';      -- 见附表,3 档
```
**目的**:输出列 ⊆ 索引列 → 免回表(覆盖)。谓词 `user_id` 正是 ORDER BY 的前导排序键(ORDER BY 的主场,二级索引最劣场景),对比**覆盖 / lookup(回表)/ ORDER BY** 三条路径。

## Q7 — 免回表 COUNT,谓词为非排序键

```sql
SELECT COUNT(*) FROM orders WHERE create_ts BETWEEN '2025-06-01 00:00:00' AND '2025-06-08 00:00:00';   -- 见附表,3 档
```
**目的**:免回表(覆盖)。谓词 `create_ts` **不在** ORDER BY 排序键内(ORDER BY 无法剪枝、被迫全表扫)——验证二级索引的**核心价值场景**:多列各建索引、对非排序键查询数量级加速。

---

## 附表 A — `user_id` 选择度档位(Q2/Q3/Q6,VARCHAR 字典序)

| 档位 | 谓词 | 命中行 | 实测选择度 |
|---|---|---|---|
| 0.0005% | `user_id = '12345'` | 540 | 0.0005% |
| 0.55% | `user_id BETWEEN '12345' AND '12444'` | 550,205 | 0.55% |
| 5.5% | `user_id BETWEEN '14000' AND '14999'` | 5,549,047 | 5.5% |
| 11.1% | `user_id BETWEEN '13000' AND '14999'` | 11,104,515 | 11.1% |
| 27.8% | `user_id BETWEEN '10000' AND '14999'` | 27,772,550 | 27.8% |

> Q6(免回表)取其中 0.55% / 5.5% / 27.8% 三档。

## 附表 B — `user_id` 选择度档位(Q5,BIGINT 数值)

| 档位 | 谓词 | 命中行 |
|---|---|---|
| 0.0005% | `user_id = 12345` | 540 |
| 0.55% | `user_id BETWEEN 12345 AND 13444` | 551,178 |
| 5.5% | `user_id BETWEEN 12345 AND 23444` | 5,553,719 |
| 11.1% | `user_id BETWEEN 12345 AND 34544` | 11,101,083 |
| 27.8% | `user_id BETWEEN 12345 AND 67944` | 27,799,616 |

## 附表 C — `create_ts` 范围档位(Q7,非排序键)

| 范围 | 谓词上界(下界固定 `2025-06-01 00:00:00`) | 命中行 |
|---|---|---|
| 1 天 | `'2025-06-02 00:00:00'` | 114,468 |
| 1 周 | `'2025-06-08 00:00:00'` | 801,786 |
| 1 月 | `'2025-07-01 00:00:00'` | 3,438,636 |

## 附表 D — 路径切换(免回表 Q6/Q7)

| 路径 | 执行对象 | 配置 |
|---|---|---|
| 覆盖(免回表) | `orders` | `enable_secondary_index_read=true` + `enable_secondary_index_covering=true` |
| lookup(回表) | `orders` | `enable_secondary_index_read=true` + `enable_secondary_index_covering=false` |
| ORDER BY(对照) | `orders_ob` | —(对照表无需索引) |
