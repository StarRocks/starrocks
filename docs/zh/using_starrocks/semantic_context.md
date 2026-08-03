---
displayed_sidebar: docs
keywords: ['semantic', 'context', 'agent', 'rag', 'graph_expand', 'context_search']
description: "StarRocks Semantic Context（AgentBase）：通过文本、向量与图检索的 context search，存储、嵌入并检索 agent 记忆与 RAG 文档。"
---

# Semantic Context

Semantic-context 模块将 StarRocks 变为 agent 应用的状态存储：版本化的 markdown 实体、引用链接、片段级文本检索、有界引用扩展、token 预算打包，全部构建在现有主键表引擎之上。

本文档是面向用户的功能概览。完整架构见仓库根目录的四份设计文档；下面的章节对应各外部契约的设计章节。

## 快速入门

```sql
-- 1. 声明 contextbase 与 collection
CREATE CONTEXTBASE sales_ai
PROPERTIES ("default_consistency" = "STRICT");

CREATE CONTEXT COLLECTION sales_ai.pipeline_rules
PROPERTIES ("collection_type" = "knowledge", "default_token_budget" = "4000");

-- 2. Upsert 一个实体
CONTEXT UPSERT INTO sales_ai.pipeline_rules
ENTITY (
    entity_key = 'smb_baseline',
    entity_type = 'page',
    title = 'SMB Baseline',
    preview = 'SMB stage duration and conversion baselines',
    content = $$---
type: page
source: [201]
---
## Stages

SMB deals spend 12 days in Prospect [[e:231]].
## Conversion

Closed-won rate for SMB is 24%.
$$
)
OPTIONS (consistency = 'STRICT');

-- 3. 观测写入状态
SHOW CONTEXT STATUS;
```

Upsert 后，五张内部表分别写入：`context_entity_versions`（一行版本）、`context_entity_heads`（一行当前头）、`context_entity_fragments`（一条 preview + N 条 section）、`context_entity_refs`（每个 `[[e:id]]` / `[[e:entity_key]]` 与 `source` 条目一行）、`context_commits`（一行 commit，统一 `snapshot_version`）。

## Markdown 引用

`[[e:<X>]]` 是从 markdown body 中引用另一个实体的语法。`<X>` 接受两种形态，最终都会落到 `context_entity_refs` 的同一种行格式：

| 形态 | 示例 | 解析方式 |
|---|---|---|
| 数字 id | `[[e:231]]` | 直接使用，不查表。 |
| `entity_key` | `[[e:smb_baseline]]` | 写入时通过 `context_entity_heads(contextbase_id, entity_key)` 解析，base-scoped — 同一 contextbase 下别的 collection 里的 key 也可被引用。 |

frontmatter 的引用列表字段（`source:`、`source_pages:`、`refs:`、`links:`、`references:`）也支持这两种形态。标量、内联列表、块列表三种 YAML 形式都可，混用也可：

```yaml
---
source: [201, smb_baseline, "enterprise_baseline", 305]
refs:
  - 401
  - planner_skill
---
```

引号字符串（`'foo'` / `"foo"`）会在分类前去掉引号。

### 严格解析

如果任何 `[[e:<key>]]` 无法解析到同一 contextbase 下的存活实体，整条 UPSERT 会以 `ENTITY_NOT_FOUND` 报错，**没有任何数据落库**。两种典型情况：

- **前向引用**：写实体 A 引用 B by key 但 B 还不存在。批量导入必须按拓扑序：先写叶子再写引用方。批量 UPSERT 路径会在 Phase 3 分配 id，因此**同一批次内**的前向引用可解析（A 在第 1 行引用 B 在第 N+1 行，两条都成功）。
- **被 tombstone 的目标**：软删除的实体不会解析成功。先用同样的 `entity_key` 再 UPSERT 一次（复活实体），再写引用方。

### 禁止：纯数字 entity_key

`entity_key` 匹配 `^\d+$`（如 `"12345"`）会在写入时被 `INVALID_ENTITY_KEY` 拒绝。这是为了消除 `[[e:12345]]` 的二义性 — 抽取器优先走数字分支，纯数字 key 也永远无法通过 markdown 被引用。合法 entity_key 必须以字母或 `_` 开头，字符集为 `[A-Za-z0-9_./:-]`：

```sql
-- OK
entity_key = 'smb_baseline'
entity_key = 'team_a.smb.baseline'
entity_key = 'agent:planner'

-- 被拒绝
entity_key = '12345'
```

### 想要显式（基于 key 的）边、绕过 markdown

SQL `EDGES (...)` 子句接受 bigint 字面量或字符串字面量；字符串走与 markdown 内联引用相同的 `entity_key` 解析路径。当引用方 body 为空 / 非 markdown 但仍需要图边时使用：

```sql
CONTEXT UPSERT INTO sales_ai.pipeline_rules
ENTITY (entity_key = 'deal_scoring', entity_type = 'page', content = 'Body without refs.')
EDGES ('smb_baseline', 'enterprise_baseline');
```

`EDGES` 行的 `ref_kind = 'explicit'`，markdown 内联引用是 `'inline'`；`graph_expand` 默认都遍历两种边。

## 对象模型

```
contextbase
  └── collection（类型：knowledge / skill / memory / task_summary / channel）
        ├── entity（逻辑标识 = id；每次写入生成新 version）
        │     ├── version 1
        │     ├── version 2
        │     └── version N
        └── workspace object / channel message / derived entity
```

各 collection 类型允许的 entity 类型由 `CollectionTypePolicy` 约束。参见架构文档 §4.2 的矩阵；analyzer 在 `CONTEXT UPSERT` 阶段会拒绝非法组合。

## 内部表

模块所有持久化都落在隐藏数据库 `__internal_context` 下的 8 张主键表。普通用户无需直接读它们——所有用户可见接口（DDL/DML、SHOW、TVF、REST）都已封装好——但运维/取证场景会用到。

| 表 | 主键 | 角色 |
|---|---|---|
| `context_entity_versions` | `(entity_id, version)` | 权威版本历史（body、raw markdown、frontmatter、source） |
| `context_entity_heads` | `(entity_id)` | 当前版本缓存，加速点读 |
| `context_entity_fragments` | `(entity_id, version, fragment_id)` | preview + section 片段；承载 `fragment_text` 上的 GIN 索引和 `embedding` 上的 HNSW 索引 |
| `context_entity_refs` | `(src_entity_id, src_version, ord)` | 引用边，驱动 `graph_expand` |
| `context_commits` | `(snapshot_version)` | 提交日志；as-of 读取使用的 snapshot fence |
| `context_workspace_objects` | `(workspace_id, object_id, version)` | per-session workspace 对象（memory / scratch / output） |
| `context_channel_subscriptions` | `(subscription_id)` | 模式化的 channel 订阅 |
| `context_tasks` | `(task_id)` | 后台任务状态，由 `SHOW CONTEXT TASKS` 暴露 |

## 权限模型

两层访问控制，全部由 analyzer / REST handler 强制；任何 write 或管理类接口都不可能绕过。

**系统级权限**——所有 CREATE / ALTER / DROP / 写入入口必备：

```sql
GRANT CREATE CONTEXTBASE ON SYSTEM TO USER <user>;   -- 管理员授权
```

它管控：`CREATE/ALTER/DROP CONTEXTBASE`、`CREATE/DROP CONTEXT COLLECTION`、`CREATE/DROP WORKSPACE`、`CREATE/DROP RETRIEVAL PROFILE`、`CONTEXT UPSERT`、`CONTEXT DELETE`、`WORKSPACE UPSERT` 以及对应的 REST 端点。内置角色 `OPERATE` 和 `SECURITY` 默认绕过此检查。

**Per-base 所有权 / 授权**——`CREATE CONTEXTBASE` 时会把 `_owner_user` 写入 metadata。后续对该 base 的 `ALTER`、`DROP` 以及任何写入都需要 ownership *或* 一个 per-base grant。授权语法：

```sql
GRANT USAGE ON CONTEXTBASE <name> TO USER <user>;    -- 读 + 写数据
GRANT ALTER ON CONTEXTBASE <name> TO USER <user>;    -- ALTER / collection 生命周期
GRANT DROP  ON CONTEXTBASE <name> TO USER <user>;    -- DROP CONTEXTBASE
REVOKE USAGE ON CONTEXTBASE <name> FROM USER <user>;
```

运维专属端点（`/api/context/health`、cluster-wide 的 `/api/context/stats`）需要系统级 `CREATE CONTEXTBASE`——它们暴露整个集群下所有 base 的计数信息，因此采用管理员门禁。

## SQL 接口

### DDL

| 语句 | 作用 |
|---|---|
| `CREATE CONTEXTBASE <name> PROPERTIES (...)` | 声明新的 contextbase，指定默认 consistency 与 embedding 模式 |
| `ALTER CONTEXTBASE <name> SET (...)` | 修改 contextbase 属性 |
| `ALTER CONTEXTBASE <name> RENAME TO <newName>` | 原地重命名 contextbase；仅重建元数据键、保留 id，因此数据与授权均不受影响 |
| `DROP CONTEXTBASE [IF EXISTS] <name>` | 删除 contextbase |
| `CREATE CONTEXT COLLECTION <cb.col>` | 声明带类型的 collection |
| `DROP CONTEXT COLLECTION [IF EXISTS] <cb.col>` | 删除 collection |
| `CREATE WORKSPACE <cb.col.ws>` | 开启会话内 scratch workspace |
| `DROP WORKSPACE [IF EXISTS] <cb.col.ws>` | 删除 workspace |
| `CREATE RETRIEVAL PROFILE <name>` | 注册 fusion 权重与检索默认值 |
| `DROP RETRIEVAL PROFILE [IF EXISTS] <name>` | 删除 retrieval profile |

### DML

| 语句 | 作用 |
|---|---|
| `CONTEXT UPSERT INTO <cb.col> ENTITY (...) EDGES (...) OPTIONS (...)` | 写入新版本实体，`EDGES` 是显式引用的兼容输入 |
| `CONTEXT DELETE FROM <cb.col> WHERE id = N \| entity_key = 'k'` | 写入一个保留正文/来源链的新当前版本，并把 `confidence` 设为 `0.0`（soft delete） |
| `WORKSPACE UPSERT INTO <cb.col.ws> OBJECT (...)` | 写 workspace 内对象，并通过 `workspace_scope` 显式指定 `memory` / `scratch` / `output` 路由 |

REST 层还在同一存储模型之上暴露了类文件系统 CRUD 语义：

- `POST /api/context/get` 支持 `fields`、`options = "-L..."`、`options = "--history"`、`version`、`as_of_time`。`-L` 只按 **body 行号** 计数，并返回切片后的正文。
- `POST /api/context/upsert` 既支持完整 `entity` upsert，也支持基于已有 `id` / `entity_key` 的 `write_options` 更新（`-a`、`-L10-20`、`-L15i`），这些更新都只作用在 body。
- `POST /api/context/delete` 支持 `hard_delete=true` 做物理删除；默认 soft delete 会保留 body / raw markdown / frontmatter / source，仅把当前版本降级为 `confidence = 0.0`。

### SHOW

| 语句 | 作用 |
|---|---|
| `SHOW CONTEXTBASES` | 列出所有 contextbase，并返回 collection 数、entity 数、更新时间、状态与默认属性 |
| `SHOW COLLECTIONS FROM <cb>` | 列出 collection，含类型、entity 数、更新时间、状态与 retrieval profile |
| `SHOW WORKSPACES FROM <cb>` | 列出开启的 workspace，并返回 `memory` / `scratch` / `output` 计数与最后活动时间 |
| `SHOW CONTEXT STATUS` | 实时计数：contextbases、collections、workspaces、entities、versions、fragments、refs、commits |
| `SHOW CONTEXT TASKS` | `context_tasks` 中的后台任务状态 |
| `SHOW CONTEXT CONSISTENCY` | commit 行的 visibility 状态 |
| `SHOW CONTEXT PROFILE [name]` | Retrieval profile 配置 |

### Table function（TVF）

模块对外暴露 9 个 SQL 表函数。任何接受关系的 SQL 位置都能调用：

```sql
SELECT * FROM TABLE(context_get(123));
SELECT id, entity_key, text_score
  FROM TABLE(text_search(contextbase => 'sales_ai', pattern => 'deal scoring', `limit` => 20));
```

每个 TVF 同时支持位置参数（兼容形态）与命名参数（完整能力）。FE 在 analyze 阶段把它们 rewrite 成可执行 SQL 或 ValuesRelation，BE 端的 `ContextGet` 算子始终是 schema-only fallback。下表的命名参数中，`scope`、`contextbase`、`collection`、`collections`、`collection_type`、`as_of_time`、`snapshot_version` 在所有需要 scope 的位置都通用。

#### 读取类 TVF

| TVF | 位置参数 | 关键命名参数 | 输出列 |
|---|---|---|---|
| `context_get(<id>)` / `context_get(<entity_key>)` | 一个 BIGINT id 或 STRING key | `version`、`as_of_time`、`level={preview,standard,deep}`、`neighbor_limit`、`options`（如 `-L10-20`、`--history`） | `id`、`entity_key`、`entity_type`、`title`、`body`、`preview`、`raw_markdown`、`version`、`updated_time`、`created_time`、`snapshot_version`、`source`、`deleted` |
| `entity_history(<id>)` | 一个 BIGINT entity id | — | `id`、`version`、`snapshot_version`、`updated_time`、`deleted`、`preview`、`confidence` |
| `read_collection(<collection_id>)` | 一个 BIGINT collection id | `snapshot_version`、`as_of_time`、`limit`（默认 1000） | 18 列实体导出（`id`、`version`、`entity_key`、`entity_type`、`contextbase_id`、`collection_id`、`title`、`preview`、`body`、`raw_markdown`、`frontmatter_json`、`source`、`confidence`、`created_time`、`updated_time`、`commit_time`、`snapshot_version`、`deleted`） |
| `read_contextbase(<contextbase_id>)` | 一个 BIGINT contextbase id | `snapshot_version`、`as_of_time`、`limit`（默认 2000） | 与 `read_collection` 一致 |

`read_collection` 与 `read_contextbase` 会被 rewrite 成针对 `__internal_context.context_entity_heads JOIN context_entity_versions` 的 SubqueryRelation，行扫描在 BE 原生执行，不再走 FE 堆。

#### 检索类 TVF

| TVF | 位置参数 | 关键命名参数 | 输出列 |
|---|---|---|---|
| `text_search(<contextbase_id>, <pattern>)` | BIGINT id + STRING pattern | `entity_type`、`confidence_min`、`limit`、`offset`、`options`（`-A/-B/-C/-n/-i/-c/-l`） | `id`、`entity_key`、`entity_type`、`version`、`snapshot_version`、`preview`、`confidence`、`hit_count`、`text_score`、`top_snippet`、`snippet_fragment_kind`、`line_start`、`line_end` |
| `vector_search(<contextbase_id>, <query_text>)` | BIGINT id + STRING text | `query_embedding`、`entity_type`、`confidence_min`、`limit`、`offset`、`options="-d"`（deep）、`allow_stale_vector` | `id`、`entity_key`、`entity_type`、`preview`、`version`、`snapshot_version`、`confidence`、`vector_score`、`matched_fragment_kind`、`matched_snippet` |
| `context_search(<contextbase_id>, <query_text>)` | BIGINT id + STRING text | `query_embedding`、`seed_ids`（可选；省略时按 text/vector 命中自动派生）、`graph_seed_topk`、`max_results`、`max_tokens`、`graph_mode={AUTO,OFF}`、`text_weight`、`vector_weight`、`graph_weight`、`graph_depth`、`max_frontier`、`edge_types`、`direction={FORWARD,BACKWARD,BOTH}`、`retrieval_profile`、`consistency`、`workspace`、`allow_stale_vector` | `id`、`entity_key`、`entity_type`、`title`、`preview`、`version`、`snapshot_version`、`final_score`、`text_score`、`vector_score`、`graph_score`、`hop_count`、`edge_types`、`snippet` |

#### 图扩展 / 打包类 TVF

| TVF | 位置参数 | 关键命名参数 | 输出列 |
|---|---|---|---|
| `graph_expand(<seed_id>, <depth>)` | BIGINT seed + INT depth | `seed_ids` / `seeds`、`direction`、`max_depth`、`edge_types`、`max_frontier`、`require_complete` | `seed_id`、`id`、`entity_key`、`hop`、`path_score`、`edge_types`、`path_meta`、`snapshot_version` |
| `context_pack(<contextbase_id>, <max_tokens>)` | BIGINT id + BIGINT max_tokens | `entity_ids`、`include_citations`、`max_tokens` | `packed_text`、`used_tokens_estimate`、`included_entities`、`truncated_entities`、`citations` |

## REST 接口

REST 层与 SQL 层对齐，并额外提供批量 + 检索类接口。

### 管理类

- `POST /api/contextbases` — body `{"name": "...", "properties": {...}}`
- `GET /api/contextbases` — 返回 `collection_count`、`entity_count`、`updated_time`、`status`
- `DELETE /api/contextbases/{name}[?if_exists=true]`
- `POST /api/collections` — body `{"contextbase": "...", "name": "...", "collection_type": "knowledge", "properties": {...}}`
- `GET /api/collections[?contextbase=<name>&collection_type=<type>]` — 返回 `entity_count`、`updated_time`、`status`
- `POST /api/workspaces`
- `GET /api/workspaces[?contextbase=<name>]` — 列出 workspace，并返回 `memory` / `scratch` / `output` 计数和 `last_activity`
- `POST /api/retrieval-profiles`
- `GET /api/context/health` — 仅运维可用的就绪探针（`is_leader`、`internal_tables_ready`、4 个元数据计数 + 4 个内部表行数）。即使部分未就绪也返回 200，便于看板渲染形状而非二元 up/down。
- `GET /api/context/stats[?contextbase=<name>]` — 容量指标。带 `contextbase` 参数时只要求对该 base 的 `USAGE`；不带参数的 cluster-wide 形态为管理员专用。底层表尚未物化时该项返回 `-1`，便于看板显示 “n/a”。

#### Workspace 生命周期

Workspace 是 collection 下的会话级 scratch 区。生命周期端点把 “开启 → 提交 / 丢弃” 流程显式建模；`POST /api/workspaces` 是直接创建的旧路径。

- `POST /api/workspaces/start` — body `{"qualified_name": "<cb.col.ws>", "collection_id": <id>, "properties": {...}}`。幂等：已有 workspace 走 resume（返回原 id 和 `resumed=true`），不存在则新建。响应同时返回 `memory` / `scratch` / `output` 计数与 `last_activity`，agent 可据此判断是否继续在 resume 状态下工作。
- `POST /api/workspaces/commit` — body `{"qualified_name": "<cb.col.ws>", "target_collection": "..."}`。把所有未 tombstone 的 workspace 对象 promote 到目标 collection。该接口投递一个 `WORKSPACE_COMMIT` 后台任务并返回任务 id；调用方可通过 `SHOW CONTEXT TASKS` 轮询完成情况。
- `POST /api/workspaces/discard` — body `{"qualified_name": "<cb.col.ws>"}`。先把 workspace 中所有当前活跃对象 tombstone，再删除 workspace metadata。共享 collection 不受影响。**不可逆**。

### 数据类

- `POST /api/context/upsert` — 单行 upsert，或基于 `id` / `entity_key` 的 write-style 更新
- `POST /api/context/bulk-import` — 批量 upsert，单行失败不影响批次。整批在**同一个** `snapshot_version` 下提交（只写一行 `context_commits`），因此 as-of 读取要么看到整批、要么一行都看不到；每一行的返回结果都携带这同一个 `snapshot_version`。
- `POST /api/context/delete` — 默认 soft delete；`hard_delete=true` 时做物理删除
- `POST /api/context/bulk-delete` — 批量 soft delete。body `{"selectors": [{"id": 301}, {"entity_key": "smb_baseline"}, ...]}`。每行独立 tombstone；只带 `entity_key` 的 selector 会被解析成 `id`，解析失败的行在响应里附带 per-row error，不会让整个批次失败。
- `POST /api/context/get` — body `{"id": 123}` 或 `{"entity_key": "..."}` 或 `{"id": 123, "version": 5}`；同时支持 `as_of_time`、`fields`、`options`、`level`
- `POST /api/context/history` — 一个实体的全部版本（等价于 `/api/context/get` 上的 `options="--history"`）
- `POST /api/context/read-collection` — 整 collection 导出，可传 `as_of_time`；返回完整实体 payload（body、raw markdown、frontmatter、source、时间戳、confidence、vector 状态）。按 `limit` + `offset` 或 `after_entity_id`（游标）分页，见下方[分页](#分页)。
- `POST /api/context/read-contextbase` — 整 contextbase 导出，可传 `as_of_time`；返回同样的完整实体 payload。分页契约与 `read-collection` 相同。
- `POST /api/workspace/upsert` — 支持 `workspace_scope = memory|scratch|output`

#### Disclosure level

`POST /api/context/get` 与 `context_get` TVF 接受 `level` 参数，用于控制单实体返回的 payload 详细程度：

| Level | 返回内容 |
|---|---|
| `preview`（廉价读默认） | 跳过 versions 表 join，仅返回 head 缓存的标量（preview、title、confidence、snapshot version 等）。 |
| `standard` | 通过 `ContextReadExecutor.getNeighbourPreviews` 额外返回邻居 preview。 |
| `deep` | 进一步通过 `getNeighbourBodies` 拉取一跳邻居的 body。**慎用**——body 可能很大。 |

#### 分页

`POST /api/context/read-collection` 与 `POST /api/context/read-contextbase` 的结果集按 `entity_id ASC` 排序（FE 单调唯一分配，分布式计划下排序键全序、tiebreak 确定）。提供两种分页原语：

| 参数 | 含义 |
|---|---|
| `limit` | 每页最大行数（read-collection 默认 500，read-contextbase 默认 1000）。 |
| `offset` | 跳过前 N 行。每次调用代价 `O(offset + limit)` —— MPP 计划必须在 coordinator BE 把前缀按序物化后再丢弃。**小 collection / 偶发巡检可用，大 collection 长链路翻页不要用**（跨页累计 O(N²)）。 |
| `after_entity_id` | Keyset 游标 —— 仅返回 `entity_id > after_entity_id` 的行。每次调用 `O(log N + limit)`（PK 范围扫描，无前缀物化）。**所有跨多页的扫描首选这个。** 与 `offset` 同时给时，`after_entity_id` 优先。 |

响应中带 `next_after_entity_id` —— 本页最后一行的 `entity_id`，本页行数不足 `limit`（即扫到末尾）时为 `null`。游标循环判 null 即停：

```bash
# 整 collection 走一遍
cursor=null
while :; do
  body=$(jq -n --argjson c "$cursor" '{contextbase:"cb",collection:"docs",limit:500} + (if $c == null then {} else {after_entity_id:$c} end)')
  resp=$(curl -s -X POST $URL/api/context/read-collection -H 'Content-Type: application/json' -d "$body")
  echo "$resp" | jq -c '.rows[]'              # 消费行
  cursor=$(echo "$resp" | jq '.next_after_entity_id')
  [ "$cursor" = "null" ] && break
done
```

为什么不能只用 offset？分布式 MPP 计划下，`LIMIT N OFFSET M` 要求 coordinator BE 先把前 `M + N` 行按序合并物化才能丢掉前 `M` —— 代价随 offset 线性增长；并且排序键必须是 unique 全序，否则不同 BE 的并列项次序在重试间不一致（这次的 `next_after_entity_id` 就是为修这个 bug 而引入）。基于 PK 的游标避开了这两条：`WHERE entity_id > $cursor` 谓词被下推到 OlapScan 配合 `LIMIT N` early-stop；并发写也无法挪动已翻过的游标，因为新分配的 `entity_id` 永远更大。

### 检索类

- `POST /api/context/search` — text + vector + reference 融合，含 explain；支持 `scope`、`collections`、`collection_type`、`workspace`、`retrieval_profile`、`consistency`、`as_of_time`。`max_tokens` 现在会驱动预算感知打包结果（`packed_text`、`used_tokens_estimate`、`included_entities`、`truncated_entities`、`disclosure_levels`）。
- `POST /api/context/graph-expand` — 有界 BFS 扩展引用（`GRAPH_EXPAND` 兼容名）；支持 `scope`、`collections` 或 `collection_type`
- `POST /api/context/pack` — token 预算打包
- `POST /api/context/text-search` — 实体级命中，并真实支持 grep 风格 `-A/-B/-C/-n/-i/-c/-l` snippet 行展开
- `POST /api/context/vector-search` — 默认命中 preview embedding，`options="-d"` 时命中 section embedding。用户可见执行路径由 FE materializer 提供，BE TVF 仅保留 schema-compatible fallback。

#### Search explain 输出

`POST /api/context/search` 在返回的 `candidates` 之外还会输出 `explain` 对象。字段：

| 字段 | 含义 |
|---|---|
| `contextbase`、`collection` | 经 `scope` / `contextbase` / `collection` / `collections` / `collection_type` 解析后的 scope。 |
| `vector_path_status` | 取值之一：`executed`、`skipped_no_query`、`skipped_no_provider`、`degraded_stale`。 |
| `weights` | `{text, vector, graph}`——融合实际使用的权重（已应用 profile 自动绑定）。 |
| `retrieval_profile`、`profile_auto_bound` | 应用的 profile 名；当 profile 来自所选 collection 的 `retrieval_profile` 属性时为 `true`。 |
| `graph_mode`、`graph_depth`、`max_frontier` | 实际生效的图扩展上限。 |
| `reference_direction` | 实际使用的引用扩展 BFS 方向（`FORWARD`、`BACKWARD` 或 `BOTH`）。默认 `BOTH`，详见下文 `direction`。 |
| `graph_status` | `ran`、`skipped_off`（mode=OFF）或 `skipped_no_seeds`（既无显式种子也无可派生种子）。 |
| `graph_seeds_source` | `derived`（来自 text/vector top-K 自动派生）、`explicit`（调用方提供的 `seed_ids`）、`mixed` 或 `none`。 |
| `graph_seed_count`、`graph_seed_topk_used` | 最终种子集大小及解析后的 Top-K 上限。 |
| `synthesis_filtered_seeds` | 因为是综述实体（如 `derived_page`）而被过滤掉的种子候选数 —— 详见下方"综述降权"。 |
| `degrade_reason` | 当响应被截断或某条 path 被跳过时填入（如 `FRONTIER_LIMIT_EXCEEDED`、`VECTOR_NOT_READY`）。 |
| `snapshot_fence` | 该次读 pin 住的 `snapshot_version`。 |

#### 图融合：自动派生种子

`CONTEXT_SEARCH` 不要求调用方提供 `seed_ids`。`graph_mode=AUTO`（默认）时，融合阶段会按 text/vector 部分得分（`text_weight*text_score + vector_weight*vector_score`）从候选中挑出 Top-K 作为图扩展的种子，喂给 reference expansion 路径。这样在调用方只持有自然语言查询的常见场景下，`graph_weight` 才能真正参与最终排序。

- `graph_mode=AUTO`（默认）—— 从 text/vector top-K 派生种子（与显式 `seed_ids` 取并集），运行 reference expansion；如果种子集为空（既无命中也无显式种子），路径静默跳过，`graph_status="skipped_no_seeds"`。
- `graph_mode=OFF` —— 跳过 reference expansion；所有候选 `graph_score=0`。
- `graph_seed_topk`（可选，默认 `min(max_results, 10)`）—— 控制有多少 text/vector 候选用于种子派生。值越小，frontier 越小；值越大，适合做程序化 profiling。
- `direction`（可选，默认 `BOTH`）—— 引用扩展 BFS 的方向，取值 `FORWARD`、`BACKWARD`、`BOTH`。边的存储语义是 `src=文档 → dst=被引用实体`，因此两篇共享同一被引用实体的文档（`doc1 → entityX ← doc2`）只有在 `BOTH` 下才能互相到达；`FORWARD` 单向扩展会漏掉第二篇文档，使 `graph_weight` 贡献约等于 0。默认值由 FE 配置 `context_search_default_graph_direction` 控制；设为 `direction=FORWARD` 可恢复旧的单向行为。`BOTH` 每跳的扩展开销约翻倍，并可能更早触发 `max_frontier` 截断。
- `seed_ids`（可选）—— 高级用户的覆写参数，与自动派生的种子做并集去重。绝大多数调用方无需设置。

如果你已经知道目标实体、需要做纯图遍历，请改用独立的 `CONTEXT_GRAPH_EXPAND` TVF / `POST /api/context/graph-expand` 接口。该接口接受 `require_complete=true`，让截断以错误形式抛出，便于严格调用方处理。

> 说明：`graph_mode=REQUIRED` 在融合获得自动派生种子能力后已被移除，传入会被 `INVALID_ARGUMENT` 拒绝。需要严格语义请改用 `graph_expand` 接口的 `require_complete=true`。

#### 综述降权（`derived_page` 类实体）

综述实体（目前仅 `derived_page`）是多个 leaf 实体的聚合产物。它们的 preview/title 容易被 text 检索匹配，向量也容易匹配（综述写得好），并且它们在引用图里是天然的 hub —— 三个信号都把它们推向第一名，但 agent 真正需要的是 leaf 级证据来 ground 答案。融合检索通过三层防御处理这种情况，所有判断都收敛到 `CollectionTypePolicy.isSynthesisType()`：

- **种子过滤** —— 综述实体绝不会被选作图扩展的种子。从它们扩展只会走回 text/vector 已经发现的 leaf，并把综述自己的 `graph_score` 从汇合路径上吹高。被过滤的数量记入 `explain.synthesis_filtered_seeds`。
- **graph_score 降权** —— 综述实体的 `graph_score` 在加权前先乘 `0.5`（`SYNTHESIS_GRAPH_SCORE_FACTOR`），打破三路信号对 hub 实体的相关放大。
- **final_score 降权** —— 综述实体的 `final_score` 再乘 `0.9`（`SYNTHESIS_FINAL_SCORE_FACTOR`），叶子优先 tiebreak。
- **预算升级顺序** —— 把响应打包到 `max_tokens` 时，先把 leaf 从 PREVIEW 升到 STANDARD/DEEP，剩余预算才用于升级综述。这保证 `packed_text` 里任何综述都伴随足够的 leaf 证据，agent 不会"看完综述就停"。

### 协作类

协作（channel）层是基于同一份主键存储的多 agent 消息总线，订阅持久化在 `__internal_context.context_channel_subscriptions` 表（PK `subscription_id`）中，可在 leader 切换后恢复。

- `POST /api/context/subscribe` / `POST /api/context/unsubscribe` — body `{"subscriber": "...", "pattern": "<glob>"}`。模式区分大小写，glob 风格——`*` 与 `?` 在单段内匹配，`**` 跨段匹配。一个 subscriber 可持有多个模式，重复会自动合并。
- `POST /api/channel/send` — body `{"channel": "<cb.col>", "key": "...", "payload": "..."}`。会唤醒所有 leader-local 上正在等待匹配 pattern 的 puller。
- `POST /api/channel/pull` — body `{"channel": "...", "subscriber": "...", "since_id": <opt>, "wait_timeout_ms": <opt>}`。传 `subscriber` 走存储订阅；不传则保留原始 collection 轮询兼容路径。`wait_timeout_ms` 启用 leader-local long polling，使发送端能在不依赖 WebSocket/SSE 服务的情况下唤醒等待中的 pull。

## 检索 + 打包示例

```bash
# 写入若干文档
curl -X POST http://localhost:8030/api/context/upsert -d '{
  "contextbase": "sales_ai",
  "collection": "pipeline_rules",
  "entity": {"entity_key": "smb_baseline", "entity_type": "page",
             "content": "SMB deals close in 30 days on average. [[e:42]]"}
}'

# 检索
curl -X POST http://localhost:8030/api/context/search -d '{
  "contextbase": "sales_ai",
  "collection": "pipeline_rules",
  "query_text": "SMB deals",
  "max_results": 10,
  "graph_mode": "AUTO"
}'

# 在预算内打包 top-K。`contextbase` 必填：服务端会校验每个 entity 都属于
# 调用者有权读取的 contextbase。
curl -X POST http://localhost:8030/api/context/pack -d '{
  "contextbase": "sales_ai",
  "entity_ids": [301, 302, 303],
  "max_tokens": 4000
}'
```

## as-of 时间旅行

每个 commit 都带有单调递增的 `snapshot_version`。

- 点读（`/api/context/get`）中的 `as_of_time` 走 **entity-history 语义**，解析为 `updated_time <= as_of_time` 的最新版本。
- collection / contextbase / search / graph / vector / text 读取仍使用统一 **snapshot fence**，保证跨实体的一致视图。

```bash
curl -X POST http://localhost:8030/api/context/get -d '{
  "contextbase": "sales_ai",
  "collection": "pipeline_rules",
  "entity_key": "smb_baseline",
  "as_of_time": "2026-03-01",
  "level": "standard"
}'
```

```bash
curl -X POST http://localhost:8030/api/context/read-contextbase -d '{
  "contextbase": "sales_ai",
  "as_of_time": "2026-03-01",
  "limit": 1000
}'
```

在基于 snapshot 的接口上，`as_of_time` 支持 `YYYY-MM-DD`、`YYYY-MM-DD HH:mm:ss`，或直接传 snapshot 数字。

## 错误响应

模块内部所有失败的 REST 接口返回结构化错误信封，便于客户端构建重试 / 降级而不必解析自由文本：

```json
{
  "error_code":         "FRONTIER_LIMIT_EXCEEDED",
  "error_class":        "resource",
  "message":            "graph expansion exceeded max_frontier=200 at depth 3",
  "retryable":          true,
  "degrade_suggestion": "raise max_frontier, narrow seed_ids, or unset require_complete on graph_expand",
  "request_id":         "9f1c..."
}
```

10 个稳定错误码：

| `error_code` | `error_class` | 是否可重试 | 典型原因 |
|---|---|---|---|
| `INVALID_ARGUMENT` | parameter | 否 | 命名参数名错、类型错或必填字段缺失。 |
| `INVALID_SCOPE` | parameter | 否 | `scope` / `contextbase` / `collection` / `collection_type` 同时填了多项（或全部为空）。 |
| `INVALID_COLLECTION_TYPE` | parameter | 否 | `collection_type` 不属于 `knowledge` / `skill` / `memory` / `task_summary` / `channel`。 |
| `INVALID_ENTITY_TYPE` | parameter | 否 | `entity_type` 不在该 collection 类型矩阵允许的集合内。 |
| `ENTITY_NOT_FOUND` | semantic | 否 | `id` / `entity_key` 解析不到。 |
| `WORKSPACE_EXPIRED` | semantic | 否 | workspace 已被 discard，需要 `POST /api/workspaces/start` 重新开启。 |
| `TOKEN_BUDGET_EXCEEDED` | resource | 否 | `context_pack` / `context_search` 打包视图无法塞入 `max_tokens`。 |
| `FRONTIER_LIMIT_EXCEEDED` | resource | 是 | `graph_expand` / `context_search` 的图路径超出 `max_frontier`。 |
| `REFERENCE_INDEX_NOT_READY` | consistency | 是 | `context_entity_refs` 还未追上；可设置 `graph_mode=OFF`，或等 refs 落地后重试。 |
| `VECTOR_NOT_READY` | consistency | 是 | 仅给 `query_text` 但未配置 provider，或 embedding 仍在异步生成中。 |

TVF 接口在出错时抛出同样的 `SemanticException` / `ContextException`，并在 SQL 层透出错误码。

## 可观测性

- **指标**（在 `MetricRepo` 中注册，通过 Prometheus 采集）：
  - `context_upsert_total`、`context_delete_total`、`context_get_total`
  - `context_search_total`、`context_graph_expand_total`、`context_pack_total`、`context_text_search_total`、`context_vector_search_total`
  - `context_channel_send_total`、`context_channel_pull_total`、`context_workspace_upsert_total`
- **审计日志**：`CONTEXT UPSERT/DELETE` 与 `CONTEXT_SEARCH` 在 `internal.base` audit 流输出 request_id、scope、关键参数。

## 前置条件

内部 fragments 表的 `fragment_text` 列上建有内联 GIN 倒排索引，`embedding` 列上建有向量索引。向量索引始终可用；GIN 索引仍要求开启 FE 实验配置 `enable_experimental_gin`，其默认值为 `false`。如果该开关关闭，引导 daemon 只会创建 `__internal_context` 库、但跳过所有 fragment/索引表——此时 CONTEXT UPSERT 会静默丢弃 fragments，所有检索结果都为 0。恢复方法：将 `enable_experimental_gin` 设为 `true`（`ADMIN SET FRONTEND CONFIG` 会在一个 60s 的 daemon tick 内生效，无需重启 FE），并将其写入 `fe.conf` 以在重启后保留。

## Embedding provider 配置

Embedding provider 的 endpoint、model、dimensions、timeout、api_key 是通过 SQL 管理的对象，持久化在 FE 元数据 journal / image 中，不再是 FE 配置项。通过 [`CREATE / ALTER / DROP / SHOW / DESC / SET DEFAULT EMBEDDING PROVIDER`](../sql-reference/sql-statements/cluster-management/embedding/CREATE_EMBEDDING_PROVIDER.md) 管理。所有 CONTEXT 写入路径在请求时解析当前 `DEFAULT EMBEDDING PROVIDER`；未设置则 `CONTEXT UPSERT` 与基于 `query_text` 的 `vector_search` 都会以 `VECTOR_NOT_READY` 失败。

初始化示例（仅 admin —— SYSTEM 级 `OPERATE` 权限）：

```sql
CREATE EMBEDDING PROVIDER openai
PROPERTIES (
    "endpoint"   = "https://api.openai.com/v1/embeddings",
    "model"      = "text-embedding-3-small",
    "dimensions" = "1536",
    "timeout_ms" = "15000",
    "api_key"    = "sk-..."
);
SET openai AS DEFAULT EMBEDDING PROVIDER;
```

剩余两项影响 embedding 列的 FE 配置仍在 `fe.conf` 中（也可通过 `ADMIN SET FRONTEND CONFIG` 在线调整）：

| 配置 | 默认值 | 含义 |
|---|---|---|
| `context_vector_index_dim` | 1536 | `context_entity_fragments` 首次建表时写入 inline HNSW 索引的 embedding 向量维度。必须与 `DEFAULT EMBEDDING PROVIDER` 输出维度一致，否则 embedding 写入会失败。必须在首次 leader 启动前设置 —— cloud-native 表禁止 `ALTER TABLE ADD INDEX USING VECTOR`，建表后再改无效。 |
| `context_vector_index_metric` | `cosine_similarity` | inline HNSW 索引使用的距离度量。可选 `cosine_similarity` / `l2_distance` / `inner_product`；留空则不在 DDL 中包含向量索引子句。首次 leader 启动后修改无效。 |

## 限制与路线图

- **执行归属**：支持的 SQL TVF 现在都在 FE 中通过与 REST 相同的 semantic-context service/materializer 执行。BE 里的 `ContextGet` 仅保留为 schema 兼容 fallback，用于 introspection 与未来原生算子扩展，不再是用户可见的执行路径。
- **向量检索**：独立 `/api/context/vector-search` 与融合式 `CONTEXT_SEARCH` 现在都走真实向量路径。若请求显式提供 `query_embedding`，即使没有配置 embedding provider 也能执行；若只提供 `query_text`，仍需要 provider 生成查询向量。
- **倒排索引下推**：文本检索对单 token 的 `MATCH` 查询会下推到 GIN 倒排索引；多 token / 通配模式仍回退到 `LIKE` 扫描以保持语义一致。
- **真实 embedding 提供方**：通过 `CREATE EMBEDDING PROVIDER ...` 注册并 `SET <name> AS DEFAULT EMBEDDING PROVIDER`（详见上文「Embedding provider 配置」）。未设置 default provider 时，`CONTEXT UPSERT` 会因 fragments INSERT 需要真实 embedding 而按行失败，基于 `query_text` 的 `vector_search` 也无法生成新的查询向量。endpoint 可指向任何 OpenAI 兼容的 `/v1/embeddings` 服务；provider 的 `dimensions` 必须与 `context_vector_index_dim` 一致。

## 在 BE 上计算 embedding

按行的 embedding 计算在 BE 通过标量函数 `embedding(text VARCHAR, config JSON) -> ARRAY<FLOAT>` 执行。批量插入与 SQL 驱动的查询向量化都会在所有 BE 节点上并行计算，FE leader 不再是串行瓶颈。函数采用与 FE provider 一致的 OpenAI 兼容 `/v1/embeddings` 报文格式：

```sql
SELECT embedding(
  'hello world',
  parse_json('{
    "endpoint":   "https://api.openai.com/v1/embeddings",
    "model":      "text-embedding-3-small",
    "dimensions": 1536,
    "timeout_ms": 15000,
    "api_key":    "sk-..."
  }')
);
```

`config` 参数是可选的。省略时，FE 会解析当前的 `DEFAULT EMBEDDING PROVIDER` 并自动注入其配置，因此只要集群存在默认 provider，单参数调用即可：

```sql
-- 等价于 embedding('hello world', parse_json('<默认 provider 配置>'))
SELECT embedding('hello world');
```

若未设置 `DEFAULT EMBEDDING PROVIDER`，单参数形式会在分析阶段报错，并提示使用 `CREATE EMBEDDING PROVIDER` / `SET <name> AS DEFAULT EMBEDDING PROVIDER`。如需对非默认 provider 计算 embedding，请显式传入 `config`。

内部调用方（`ContextWriteExecutor`、`VectorSearchExecutor`）会基于当前 `DEFAULT EMBEDDING PROVIDER` 物化同样形态的 JSON —— 当 provider 未配置 `api_key`（本地 / 自部署 endpoint 通常不需要 `Authorization` 头）时，`api_key` 字段会被整体省略。

### API key 处理

API key 真值持久化在 FE 元数据中（journal + image），并在 BE 端 `embedding(text, config_json)` 调用点内联使用：

- 通过 `CREATE / ALTER EMBEDDING PROVIDER` 管理 provider 凭证 —— `api_key` 随 provider 对象写入 FE meta journal 与 image，集群重启与升级都会保留该凭证。
- `SHOW EMBEDDING PROVIDERS` 与 `DESC EMBEDDING PROVIDER` 会将 api_key 屏蔽为 `******`。明文仅在 `EmbeddingConfigJson` / `FeEmbeddingClient` 内部构造给 BE 的 config JSON 时读取。
- 审计日志的脱敏器（`SqlCredentialRedactor`）会把 SQL 字符串中出现的 `"api_key":"<value>"` 全部置换为 `***`。
- 所有 `EMBEDDING PROVIDER` DDL（含 `SHOW` / `DESC`）都要求 SYSTEM 级 `OPERATE` 权限。
- 任何能读取 FE meta image / BDB journal 目录的人都能读出 provider 的 `api_key` 明文，请用文件系统权限严格保护这些目录。

未设置 `DEFAULT EMBEDDING PROVIDER` 时，所有需要 embedding 的写入路径会立即以 `VECTOR_NOT_READY` 失败。

config JSON 由通用的 `ModelConfig` 解析器解析——同一份解析器也支撑 `ai_query(text VARCHAR, config JSON) -> VARCHAR`（opcode 200000，与 `embedding` 的 200001 相邻注册）。这意味着 `temperature`、`max_tokens`、`top_p` 在 JSON 里会被接收但在 embedding 路径上被忽略——直接复用 `ai_query` 的 config 不会报错，但这些字段不会影响 `/v1/embeddings` 请求。
