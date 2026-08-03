---
displayed_sidebar: docs
keywords: ['semantic', 'context', 'agent', 'rag', 'graph_expand', 'context_search']
description: "Semantic Context (AgentBase) in StarRocks: store, embed, and retrieve agent memory and RAG documents via text, vector, and graph context search."
---

# Semantic Context

The semantic-context module turns StarRocks into the state store for agent-based applications: versioned markdown entities, reference links, fragment-level text search, bounded reference expansion, and token-budgeted packing, all on top of the existing PK table engine.

This document is a user-facing tour of the module. The full architecture lives in the design documents at the repo root; the table of contents below maps each surface to the design section it implements.

## Quick start

```sql
-- 1. Declare a contextbase + collection
CREATE CONTEXTBASE sales_ai
PROPERTIES ("default_consistency" = "STRICT");

CREATE CONTEXT COLLECTION sales_ai.pipeline_rules
PROPERTIES ("collection_type" = "knowledge", "default_token_budget" = "4000");

-- 2. Upsert an entity
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

-- 3. Inspect what landed
SHOW CONTEXT STATUS;
```

After the upsert, five internal tables receive rows: `context_entity_versions` (one version row), `context_entity_heads` (one head row), `context_entity_fragments` (one preview + N section rows), `context_entity_refs` (one row per `[[e:id]]` / `[[e:entity_key]]` and per `source:` entry), `context_commits` (one commit row tagging the whole upsert with a fresh `snapshot_version`).

## Markdown references

`[[e:<X>]]` is the inline reference syntax for citing another entity from a markdown body. `<X>` is accepted in two shapes; both produce the same `context_entity_refs` row after resolution:

| Shape | Example | Resolution |
|---|---|---|
| Numeric id | `[[e:231]]` | Used as-is; no lookup. |
| `entity_key` | `[[e:smb_baseline]]` | Resolved at write time against `context_entity_heads(contextbase_id, entity_key)`. Base-scoped — a key from a sibling collection in the same contextbase is reachable. |

The same union applies to YAML frontmatter ref lists (`source:`, `source_pages:`, `refs:`, `links:`, `references:`). Scalar, inline-list, and block-list forms all accept either shape, and a single list may mix them:

```yaml
---
source: [201, smb_baseline, "enterprise_baseline", 305]
refs:
  - 401
  - planner_skill
---
```

Quoted strings (`'foo'` / `"foo"`) are unwrapped before classification.

### Strict resolution

If any `[[e:<key>]]` cannot be resolved to a live entity in the same contextbase, the entire UPSERT is rejected with `ENTITY_NOT_FOUND` — nothing is persisted. This applies to both:

- **Forward references**: writing entity A that cites B by key, where B does not yet exist. Bulk imports must order rows so leaves are written before citing entities, or use `CONTEXT UPSERT INTO ... BULK` (one batched lookup covers the whole batch, so intra-batch forward references resolve via Phase 3 allocation — A cites B and B is row N+1, both succeed).
- **Tombstoned targets**: a soft-deleted entity does not resolve. Reviving its `entity_key` via UPSERT before citing it is the supported recovery path.

### Banned: digit-only `entity_key`

An `entity_key` that matches `^\d+$` (e.g. `"12345"`) is rejected at write time with `INVALID_ENTITY_KEY`. Without this constraint, `[[e:12345]]` would be ambiguous (numeric id vs digit-only key) — the extractor always picks the numeric branch first, so a digit-only key could never be cited from markdown anyway. Allowed shapes start with a letter or `_` and may contain `[A-Za-z0-9_./:-]`:

```sql
-- OK
entity_key = 'smb_baseline'
entity_key = 'team_a.smb.baseline'
entity_key = 'agent:planner'

-- Rejected
entity_key = '12345'
```

### When you want explicit (key-based) edges that bypass markdown

The SQL `EDGES (...)` clause accepts either bigint literals or string literals; the latter goes through the same `entity_key` resolution path as inline markdown refs. Use this when the citing entity's body is empty / non-markdown but you still want graph edges:

```sql
CONTEXT UPSERT INTO sales_ai.pipeline_rules
ENTITY (entity_key = 'deal_scoring', entity_type = 'page', content = 'Body without refs.')
EDGES ('smb_baseline', 'enterprise_baseline');
```

`EDGES` rows land with `ref_kind = 'explicit'` instead of `'inline'`; both kinds participate in `graph_expand` by default.

## Object model

```
contextbase
  └── collection (typed: knowledge / skill / memory / task_summary / channel)
        ├── entity (logical identity = id; each write creates a new version)
        │     ├── version 1
        │     ├── version 2
        │     └── version N
        └── workspace object / channel message / derived entity
```

Entity type allowed inside a collection type is governed by `CollectionTypePolicy`. See the table in the architecture doc §4.2; the analyzer rejects illegal combinations at `CONTEXT UPSERT` time.

## Internal tables

The module persists everything in eight Primary Key tables under the hidden database `__internal_context`. You don't need to read them directly — every supported user surface (DDL/DML, SHOW, TVF, REST) goes through them — but operators do query them for forensic / observability work.

| Table | PK | Role |
|---|---|---|
| `context_entity_versions` | `(entity_id, version)` | Authoritative version history (body, raw markdown, frontmatter, source). |
| `context_entity_heads` | `(entity_id)` | Current-version cache for cheap point reads. |
| `context_entity_fragments` | `(entity_id, version, fragment_id)` | Preview + section fragments; carries the GIN index on `fragment_text` and the HNSW index on `embedding`. |
| `context_entity_refs` | `(src_entity_id, src_version, ord)` | Reference edges that drive `graph_expand`. |
| `context_commits` | `(snapshot_version)` | Commit log; the snapshot fence used by as-of reads. |
| `context_workspace_objects` | `(workspace_id, object_id, version)` | Per-session workspace objects (memory / scratch / output). |
| `context_channel_subscriptions` | `(subscription_id)` | Pattern-aware channel subscriptions. |
| `context_tasks` | `(task_id)` | Background task state surfaced by `SHOW CONTEXT TASKS`. |

## Authorization

Two layers of access control. Both are enforced by the analyzer / REST handlers — there is no way to call a write or admin endpoint without passing them.

**System-level privilege** — required for every CREATE / ALTER / DROP / write entry point:

```sql
GRANT CREATE CONTEXTBASE ON SYSTEM TO USER <user>;   -- admin grant
```

It gates: `CREATE/ALTER/DROP CONTEXTBASE`, `CREATE/DROP CONTEXT COLLECTION`, `CREATE/DROP WORKSPACE`, `CREATE/DROP RETRIEVAL PROFILE`, `CONTEXT UPSERT`, `CONTEXT DELETE`, `WORKSPACE UPSERT`, and the matching REST endpoints. The built-in roles `OPERATE` and `SECURITY` continue to bypass it.

**Per-base ownership / grants** — `CREATE CONTEXTBASE` stamps `_owner_user` into the metadata. From then on `ALTER`, `DROP`, and any write inside the base require either ownership *or* a per-base grant. The grant grammar:

```sql
GRANT USAGE ON CONTEXTBASE <name> TO USER <user>;    -- read + write data
GRANT ALTER ON CONTEXTBASE <name> TO USER <user>;    -- ALTER/COLLECTION lifecycle
GRANT DROP  ON CONTEXTBASE <name> TO USER <user>;    -- DROP CONTEXTBASE
REVOKE USAGE ON CONTEXTBASE <name> FROM USER <user>;
```

Operator-only REST endpoints (`/api/context/health`, cluster-wide `/api/context/stats`) require system-level `CREATE CONTEXTBASE` — these surface counts across every base in the deployment, so they're admin-gated.

## SQL surface

### DDL

| Statement | Purpose |
|---|---|
| `CREATE CONTEXTBASE <name> PROPERTIES (...)` | Declares a new contextbase with default consistency / embedding mode |
| `ALTER CONTEXTBASE <name> SET (...)` | Updates contextbase properties |
| `ALTER CONTEXTBASE <name> RENAME TO <newName>` | Renames a contextbase in place; a metadata-only rekey that keeps the id, so data and grants survive |
| `DROP CONTEXTBASE [IF EXISTS] <name>` | Drops a contextbase |
| `CREATE CONTEXT COLLECTION <cb.col>` | Declares a typed collection |
| `DROP CONTEXT COLLECTION [IF EXISTS] <cb.col>` | Drops a collection |
| `CREATE WORKSPACE <cb.col.ws>` | Opens a per-session scratch workspace |
| `DROP WORKSPACE [IF EXISTS] <cb.col.ws>` | Drops a workspace |
| `CREATE RETRIEVAL PROFILE <name>` | Registers fusion weights + retrieval defaults |
| `DROP RETRIEVAL PROFILE [IF EXISTS] <name>` | Drops a retrieval profile |

### DML

| Statement | Purpose |
|---|---|
| `CONTEXT UPSERT INTO <cb.col> ENTITY (...) EDGES (...) OPTIONS (...)` | Writes a new entity version. `EDGES` is a compatibility input for explicit references |
| `CONTEXT DELETE FROM <cb.col> WHERE id = N \| entity_key = 'k'` | Writes a new current version with preserved content/provenance and `confidence = 0.0` (soft delete) |
| `WORKSPACE UPSERT INTO <cb.col.ws> OBJECT (...)` | Writes a per-session object with an explicit `workspace_scope` route (`memory` / `scratch` / `output`) |

The REST layer also exposes filesystem-style CRUD semantics on top of the same storage model:

- `POST /api/context/get` accepts `fields`, `options = "-L..."`, `options = "--history"`, `version`, and `as_of_time`. `-L` selectors count **body lines only** and return the sliced body payload.
- `POST /api/context/upsert` accepts either a full `entity` payload or an existing `id` / `entity_key` plus `write_options` (`-a`, `-L10-20`, `-L15i`) for body-only append / line replace / insert-before-line updates.
- `POST /api/context/delete` accepts `hard_delete=true` for physical purge; the default soft-delete path preserves body/raw markdown/frontmatter/source and only deprecates the current version with `confidence = 0.0`.

### SHOW

| Statement | Purpose |
|---|---|
| `SHOW CONTEXTBASES` | Lists every contextbase with collection count, entity count, updated time, status, and defaults |
| `SHOW COLLECTIONS FROM <cb>` | Lists collections, with type, entity count, updated time, status, and retrieval profile |
| `SHOW WORKSPACES FROM <cb>` | Lists open workspaces with `memory` / `scratch` / `output` counts and last activity |
| `SHOW CONTEXT STATUS` | Live counts — contextbases, collections, workspaces, entities, versions, fragments, refs, commits |
| `SHOW CONTEXT TASKS` | Background task state from `context_tasks` |
| `SHOW CONTEXT CONSISTENCY` | Commit-row visibility state |
| `SHOW CONTEXT PROFILE [name]` | Retrieval profile configuration |

### Table functions (TVF)

The module ships nine SQL-callable table functions. Use them anywhere a relation is allowed:

```sql
SELECT * FROM TABLE(context_get(123));
SELECT id, entity_key, text_score
  FROM TABLE(text_search(contextbase => 'sales_ai', pattern => 'deal scoring', `limit` => 20));
```

Each TVF accepts either positional args (compatibility shape) or named args (full feature set). FE rewrites every supported call before execution, so the BE `ContextGet` operator stays a schema-only fallback. Common named args (`scope`, `contextbase`, `collection`, `collections`, `collection_type`, `as_of_time`, `snapshot_version`) are accepted everywhere a scope is meaningful.

#### Read TVFs

| TVF | Positional | Key named args | Output columns |
|---|---|---|---|
| `context_get(<id>)` / `context_get(<entity_key>)` | one BIGINT id or STRING key | `version`, `as_of_time`, `level={preview,standard,deep}`, `neighbor_limit`, `options` (e.g. `-L10-20`, `--history`) | `id`, `entity_key`, `entity_type`, `title`, `body`, `preview`, `raw_markdown`, `version`, `updated_time`, `created_time`, `snapshot_version`, `source`, `deleted` |
| `entity_history(<id>)` | one BIGINT entity id | — | `id`, `version`, `snapshot_version`, `updated_time`, `deleted`, `preview`, `confidence` |
| `read_collection(<collection_id>)` | one BIGINT collection id | `snapshot_version`, `as_of_time`, `limit` (default 1000) | 18-col entity dump (`id`, `version`, `entity_key`, `entity_type`, `contextbase_id`, `collection_id`, `title`, `preview`, `body`, `raw_markdown`, `frontmatter_json`, `source`, `confidence`, `created_time`, `updated_time`, `commit_time`, `snapshot_version`, `deleted`) |
| `read_contextbase(<contextbase_id>)` | one BIGINT contextbase id | `snapshot_version`, `as_of_time`, `limit` (default 2000) | identical to `read_collection` |

`read_collection` and `read_contextbase` rewrite to a SubqueryRelation against `__internal_context.context_entity_heads JOIN context_entity_versions`, so result rows scan natively on BE rather than materializing through FE heap.

#### Search TVFs

| TVF | Positional | Key named args | Output columns |
|---|---|---|---|
| `text_search(<contextbase_id>, <pattern>)` | BIGINT id + STRING pattern | `entity_type`, `confidence_min`, `limit`, `offset`, `options` (`-A/-B/-C/-n/-i/-c/-l`) | `id`, `entity_key`, `entity_type`, `version`, `snapshot_version`, `preview`, `confidence`, `hit_count`, `text_score`, `top_snippet`, `snippet_fragment_kind`, `line_start`, `line_end` |
| `vector_search(<contextbase_id>, <query_text>)` | BIGINT id + STRING text | `query_embedding`, `entity_type`, `confidence_min`, `limit`, `offset`, `options="-d"` (deep), `allow_stale_vector` | `id`, `entity_key`, `entity_type`, `preview`, `version`, `snapshot_version`, `confidence`, `vector_score`, `matched_fragment_kind`, `matched_snippet` |
| `context_search(<contextbase_id>, <query_text>)` | BIGINT id + STRING text | `query_embedding`, `seed_ids` (optional; auto-derived from text/vector hits when omitted), `graph_seed_topk`, `max_results`, `max_tokens`, `graph_mode={AUTO,OFF}`, `text_weight`, `vector_weight`, `graph_weight`, `graph_depth`, `max_frontier`, `edge_types`, `direction={FORWARD,BACKWARD,BOTH}`, `retrieval_profile`, `consistency`, `workspace`, `allow_stale_vector` | `id`, `entity_key`, `entity_type`, `title`, `preview`, `version`, `snapshot_version`, `final_score`, `text_score`, `vector_score`, `graph_score`, `hop_count`, `edge_types`, `snippet` |

#### Graph / pack TVFs

| TVF | Positional | Key named args | Output columns |
|---|---|---|---|
| `graph_expand(<seed_id>, <depth>)` | BIGINT seed + INT depth | `seed_ids` / `seeds`, `direction`, `max_depth`, `edge_types`, `max_frontier`, `require_complete` | `seed_id`, `id`, `entity_key`, `hop`, `path_score`, `edge_types`, `path_meta`, `snapshot_version` |
| `context_pack(<contextbase_id>, <max_tokens>)` | BIGINT id + BIGINT max_tokens | `entity_ids`, `include_citations`, `max_tokens` | `packed_text`, `used_tokens_estimate`, `included_entities`, `truncated_entities`, `citations` |

## REST surface

The REST layer mirrors the SQL surface and adds batch + retrieval operations that don't have SQL counterparts today.

### Management

- `POST /api/contextbases` — body `{"name": "...", "properties": {...}}`
- `GET /api/contextbases` — returns `collection_count`, `entity_count`, `updated_time`, `status`
- `DELETE /api/contextbases/{name}[?if_exists=true]`
- `POST /api/collections` — body `{"contextbase": "...", "name": "...", "collection_type": "knowledge", "properties": {...}}`
- `GET /api/collections[?contextbase=<name>&collection_type=<type>]` — returns `entity_count`, `updated_time`, `status`
- `POST /api/workspaces`
- `GET /api/workspaces[?contextbase=<name>]` — lists workspaces with `memory` / `scratch` / `output` counts and `last_activity`
- `POST /api/retrieval-profiles`
- `GET /api/context/health` — operator-only readiness probe (`is_leader`, `internal_tables_ready`, four metadata counts, four internal-table row counts). Returns 200 even on partial readiness so dashboards can render the shape rather than a binary up/down.
- `GET /api/context/stats[?contextbase=<name>]` — capacity metrics. Per-base form is callable by anyone holding `USAGE` on the base; cluster-wide form (no query string) is admin-only. Returns `-1` for any underlying table that hasn't materialized yet so dashboards render "n/a" instead of erroring.

#### Workspace lifecycle

Workspaces are short-lived per-session scratch areas under a collection. The lifecycle endpoints model a deliberate start → commit/discard flow; `POST /api/workspaces` is the legacy direct-create path.

- `POST /api/workspaces/start` — body `{"qualified_name": "<cb.col.ws>", "collection_id": <id>, "properties": {...}}`. Idempotent: an existing workspace resumes (returns its id, `resumed=true`); a missing one is created. Response also returns `memory` / `scratch` / `output` counts and `last_activity` so the agent can decide whether to keep working in the resumed state.
- `POST /api/workspaces/commit` — body `{"qualified_name": "<cb.col.ws>", "target_collection": "..."}`. Promotes every non-tombstoned workspace object into the target collection. The endpoint enqueues a `WORKSPACE_COMMIT` background task and returns its task id; poll `SHOW CONTEXT TASKS` for completion.
- `POST /api/workspaces/discard` — body `{"qualified_name": "<cb.col.ws>"}`. Tombstones every active object in the workspace then drops the workspace metadata. Shared collections are unaffected. Not reversible.

### Data

- `POST /api/context/upsert` — single-row upsert, or write-style update by `id` / `entity_key`
- `POST /api/context/bulk-import` — batch upsert; per-row success/failure isolation. The whole batch commits under a **single** `snapshot_version` (one `context_commits` row), so an as-of read either sees the entire batch or none of it; every per-row result reports that same `snapshot_version`.
- `POST /api/context/delete` — soft delete by default; `hard_delete=true` for physical purge
- `POST /api/context/bulk-delete` — batch soft-delete. Body `{"selectors": [{"id": 301}, {"entity_key": "smb_baseline"}, ...]}`. Each row is independently tombstoned; selectors carrying just `entity_key` are resolved to `id`, and rows that fail resolution surface in the response with a per-row error rather than aborting the batch.
- `POST /api/context/get` — body `{"id": 123}` or `{"entity_key": "..."}` or `{"id": 123, "version": 5}`; also supports `as_of_time`, `fields`, `options`, and `level`
- `POST /api/context/history` — all versions of an entity (equivalent to `options="--history"` on `/api/context/get`)
- `POST /api/context/read-collection` — full collection dump (body, raw markdown, frontmatter, source, timestamps, confidence, vector state), optional `as_of_time`. Paginates by `limit` plus either `offset` or `after_entity_id` (cursor). See [Pagination](#pagination) below.
- `POST /api/context/read-contextbase` — full contextbase dump with the same entity payload, optional `as_of_time`. Same pagination contract as `read-collection`.
- `POST /api/workspace/upsert` — accepts `workspace_scope = memory|scratch|output`

#### Disclosure level

`POST /api/context/get` and the `context_get` TVF accept a `level` parameter that controls how much per-entity payload is returned:

| Level | Returns |
|---|---|
| `preview` (default for cheap reads) | Skips the version-table join — only head-cached scalars (preview, title, confidence, snapshot version, etc.). |
| `standard` | Adds neighbour previews via `ContextReadExecutor.getNeighbourPreviews`. |
| `deep` | Adds one hop of neighbour bodies via `getNeighbourBodies`. Use sparingly — body payloads can be large. |

#### Pagination

`POST /api/context/read-collection` and `POST /api/context/read-contextbase` page over their result set ordered by `entity_id ASC` (allocated monotonically and uniquely by the FE — total order, stable tiebreak in a distributed plan). Two pagination primitives are exposed:

| Parameter | Meaning |
|---|---|
| `limit` | Max rows per page (default 500 for read-collection, 1000 for read-contextbase). |
| `offset` | Skip the first N rows. Each call costs `O(offset + limit)` because the MPP plan must materialize the prefix on the coordinator BE before discarding it. Fine for small collections / occasional spot checks; **bad for walking large collections** (cost compounds across pages). |
| `after_entity_id` | Keyset cursor — return only rows with `entity_id > after_entity_id`. Each call is `O(log N + limit)` (PK range scan, no prefix materialization). **Recommended for any walk that crosses more than a few pages.** Takes precedence over `offset` if both are supplied. |

Response carries `next_after_entity_id` — the last row's `entity_id`, or `null` when the page returned fewer than `limit` rows (end-of-scan). Drive cursor loops from this field:

```bash
# Walk every entity in a large collection.
cursor=null
while :; do
  body=$(jq -n --argjson c "$cursor" '{contextbase:"cb",collection:"docs",limit:500} + (if $c == null then {} else {after_entity_id:$c} end)')
  resp=$(curl -s -X POST $URL/api/context/read-collection -H 'Content-Type: application/json' -d "$body")
  echo "$resp" | jq -c '.rows[]'              # consume rows
  cursor=$(echo "$resp" | jq '.next_after_entity_id')
  [ "$cursor" = "null" ] && break
done
```

Why two primitives and not just offset? In a distributed MPP plan, `LIMIT N OFFSET M` requires the coordinator BE to merge-sort the first `M + N` rows in order before discarding the first `M`. Cost grows linearly with `offset`, and the sort key must be a unique total order or different replicas will resolve ties differently on retries (the very bug that drove `next_after_entity_id` to exist). Keyset cursors on the PK avoid both costs: a `WHERE entity_id > $cursor` predicate is pushed to the OlapScan with a `LIMIT N` early-stop, and concurrent upserts cannot shift the cursor because new ids are always allocated past it.

### Retrieval

- `POST /api/context/search` — text + vector + reference fusion with explain output; accepts `scope`, `collections`, `collection_type`, `workspace`, `retrieval_profile`, `consistency`, and `as_of_time`. `max_tokens` now drives a budget-aware packed view (`packed_text`, `used_tokens_estimate`, `included_entities`, `truncated_entities`, `disclosure_levels`).
- `POST /api/context/graph-expand` — bounded BFS over references (compatibility name for `GRAPH_EXPAND`); accepts `scope`, `collections`, or `collection_type`
- `POST /api/context/pack` — token-budgeted packing
- `POST /api/context/text-search` — entity-level text hits with real grep-style `-A/-B/-C/-n/-i/-c/-l` snippet behavior
- `POST /api/context/vector-search` — standalone semantic vector retrieval over preview embeddings by default and section embeddings with `options="-d"`; the FE materializer is the user-facing execution path, while the BE TVF remains schema-compatible fallback only

#### Search explain output

`POST /api/context/search` returns a `explain` object alongside `candidates`. Fields:

| Field | Meaning |
|---|---|
| `contextbase`, `collection` | Resolved scope after `scope` / `contextbase` / `collection` / `collections` / `collection_type` collapse. |
| `vector_path_status` | One of `executed`, `skipped_no_query`, `skipped_no_provider`, `degraded_stale`. |
| `weights` | `{text, vector, graph}` — the weights actually used for fusion (after profile auto-binding). |
| `retrieval_profile`, `profile_auto_bound` | Profile name applied; `true` when the profile came from the resolved collection's `retrieval_profile` property. |
| `graph_mode`, `graph_depth`, `max_frontier` | Graph expansion bounds in effect. |
| `reference_direction` | The reference-expansion BFS direction actually used (`FORWARD`, `BACKWARD`, or `BOTH`). Defaults to `BOTH`; see `direction` below. |
| `graph_status` | `ran`, `skipped_off` (mode=OFF), or `skipped_no_seeds` (no explicit and no derivable seeds). |
| `graph_seeds_source` | `derived` (auto from text/vector top-K), `explicit` (caller-supplied `seed_ids`), `mixed`, or `none`. |
| `graph_seed_count`, `graph_seed_topk_used` | Final seed-set size and the resolved Top-K cap. |
| `synthesis_filtered_seeds` | Count of synthesis (e.g. `derived_page`) candidates that scored high enough to seed graph expansion but were filtered out — see "Synthesis demotion" below. |
| `degrade_reason` | Populated when the response was truncated or a path was skipped (e.g. `FRONTIER_LIMIT_EXCEEDED`, `VECTOR_NOT_READY`). |
| `snapshot_fence` | The `snapshot_version` the read was pinned to. |

#### Graph fusion: auto-derived seeds

`CONTEXT_SEARCH` does not require the caller to supply `seed_ids`. With `graph_mode=AUTO` (default), the top text/vector candidates are picked as graph seeds by partial fusion score, then fed to the reference-expansion path. This is what makes the `graph_weight` actually contribute when the caller only has a query string.

- `graph_mode=AUTO` (default) — derive seeds from text/vector top-K (union with explicit `seed_ids` if any) and run reference expansion. If the seed set is empty (no hits, no explicit seeds), the path is skipped silently and `graph_status="skipped_no_seeds"` is reported.
- `graph_mode=OFF` — skip reference expansion; `graph_score=0` for every candidate.
- `graph_seed_topk` (optional, default `min(max_results, 10)`) — caps how many text/vector candidates seed the expansion. Smaller values keep the frontier bounded; larger values are useful for programmatic profiling.
- `direction` (optional, default `BOTH`) — reference-expansion BFS direction, one of `FORWARD`, `BACKWARD`, `BOTH`. Edges are stored as `src=document → dst=referenced entity`, so two documents that share a referenced entity (`doc1 → entityX ← doc2`) are only mutually reachable with `BOTH`; `FORWARD`-only expansion strands the second document and makes `graph_weight` contribute ~0. The default is controlled by the `context_search_default_graph_direction` FE config; set `direction=FORWARD` to restore the legacy single-direction behavior. `BOTH` roughly doubles per-hop expansion work and may hit `max_frontier` truncation sooner.
- `seed_ids` (optional) — power-user override that composes (union, dedup) with auto-derived seeds. Most callers do not need to set this.

For pure graph traversal from already-known entities, use the dedicated `CONTEXT_GRAPH_EXPAND` TVF / `POST /api/context/graph-expand` endpoint. It accepts `require_complete=true` for strict callers that want truncation to surface as an error.

> Note: `graph_mode=REQUIRED` was removed when fusion gained auto-seed-derivation — it is rejected with `INVALID_ARGUMENT`. Use the `graph_expand` endpoint with `require_complete=true` for strict semantics instead.

#### Synthesis demotion (`derived_page` and friends)

Synthesis entities — currently only `derived_page` — are aggregations of multiple leaf entities. Their preview/title text-matches the query well, vector embeddings of the synthesis match well, *and* they're hubs in the reference graph, so a naive linear fusion places them above the leaf evidence agents actually need to ground answers. Three layered defenses, all routed through `CollectionTypePolicy.isSynthesisType()`:

- **Seed filter.** A synthesis entity is never picked as a graph-expansion seed: it would just walk back to leaves we already discovered and inflate its own `graph_score` from converging inbound paths. The number of seeds filtered this way is reported in `explain.synthesis_filtered_seeds`.
- **Graph score discount.** A synthesis entity's `graph_score` is multiplied by `0.5` before entering the linear fusion sum, breaking the correlation between the three retrieval signals for hub entities. (`SYNTHESIS_GRAPH_SCORE_FACTOR`)
- **Final score discount.** A synthesis entity's `final_score` is multiplied by `0.9` after fusion — leaf-first tiebreak when scores are close. (`SYNTHESIS_FINAL_SCORE_FACTOR`)
- **Budget upgrade order.** When packing the response into `max_tokens`, leaves are upgraded from PREVIEW to STANDARD/DEEP first; synthesis entities are upgraded only with the leftover budget. This guarantees that any synthesis included in `packed_text` is accompanied by enough leaf evidence for the agent to ground its citations.

### Channel

The channel surface is a pattern-aware multi-agent message bus on top of the same Primary Key store. Subscriptions live in `__internal_context.context_channel_subscriptions` (PK `subscription_id`) and survive leader failover.

- `POST /api/context/subscribe` / `POST /api/context/unsubscribe` — body `{"subscriber": "...", "pattern": "<glob>"}`. Patterns are case-sensitive globs — `*` and `?` match single segments, `**` crosses segments. A subscriber can hold many patterns; duplicates are coalesced.
- `POST /api/channel/send` — body `{"channel": "<cb.col>", "key": "...", "payload": "..."}`. Wakes any leader-local pullers blocked on a matching pattern.
- `POST /api/channel/pull` — body `{"channel": "...", "subscriber": "...", "since_id": <opt>, "wait_timeout_ms": <opt>}`. The optional `subscriber` routes delivery through the stored subscriptions; omitting it preserves raw collection polling. `wait_timeout_ms` enables leader-local long polling so a sender wakes waiting pullers without a separate WebSocket/SSE service.

## Worked example: search + pack

```bash
# Index a few documents
curl -X POST http://localhost:8030/api/context/upsert -d '{
  "contextbase": "sales_ai",
  "collection": "pipeline_rules",
  "entity": {"entity_key": "smb_baseline", "entity_type": "page",
             "content": "SMB deals close in 30 days on average. [[e:42]]"}
}'

# Search
curl -X POST http://localhost:8030/api/context/search -d '{
  "contextbase": "sales_ai",
  "collection": "pipeline_rules",
  "query_text": "SMB deals",
  "max_results": 10,
  "graph_mode": "AUTO"
}'

# Pack the top-K into a bounded budget. `contextbase` is required so the
# server can verify every entity belongs to a base the caller is allowed
# to read.
curl -X POST http://localhost:8030/api/context/pack -d '{
  "contextbase": "sales_ai",
  "entity_ids": [301, 302, 303],
  "max_tokens": 4000
}'
```

## as-of time travel

Every commit is stamped with a monotonic `snapshot_version`.

- Point reads (`/api/context/get`) treat `as_of_time` as an **entity-history selector** and resolve the latest version whose `updated_time <= as_of_time`.
- Collection/contextbase/search/graph/vector/text reads still pin a single **snapshot fence** so cross-entity reads stay mutually consistent.

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

`as_of_time` accepts `YYYY-MM-DD`, `YYYY-MM-DD HH:mm:ss`, or a raw snapshot number on snapshot-based surfaces.

## Error responses

Every REST endpoint that fails inside the module returns a structured envelope so SDK clients can build retry/backoff without scraping free-form text:

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

The 10 stable error codes:

| `error_code` | `error_class` | Retryable | Typical cause |
|---|---|---|---|
| `INVALID_ARGUMENT` | parameter | no | Bad named-arg, wrong type, missing required field. |
| `INVALID_SCOPE` | parameter | no | More than one of `scope` / `contextbase` / `collection` / `collection_type` was supplied (or all empty). |
| `INVALID_COLLECTION_TYPE` | parameter | no | `collection_type` not one of `knowledge` / `skill` / `memory` / `task_summary` / `channel`. |
| `INVALID_ENTITY_TYPE` | parameter | no | `entity_type` not allowed by the collection's `collection_type` matrix. |
| `ENTITY_NOT_FOUND` | semantic | no | `id` / `entity_key` does not resolve. |
| `WORKSPACE_EXPIRED` | semantic | no | The workspace was discarded; restart via `POST /api/workspaces/start`. |
| `TOKEN_BUDGET_EXCEEDED` | resource | no | `context_pack` / `context_search` packed view could not fit `max_tokens`. |
| `FRONTIER_LIMIT_EXCEEDED` | resource | yes | `graph_expand` / `context_search` graph path exceeded `max_frontier`. |
| `REFERENCE_INDEX_NOT_READY` | consistency | yes | `context_entity_refs` hasn't caught up; set `graph_mode` to `OFF` or retry once the refs settle. |
| `VECTOR_NOT_READY` | consistency | yes | `vector_search` over `query_text` and no provider configured, or embedding is still pending. |

The TVF surface raises the same errors as `SemanticException` / `ContextException` and surfaces them through the SQL layer.

## Observability

- **Metrics** (all registered in `MetricRepo`, Prometheus-scraped):
  - `context_upsert_total`, `context_delete_total`, `context_get_total`
  - `context_search_total`, `context_graph_expand_total`, `context_pack_total`, `context_text_search_total`, `context_vector_search_total`
  - `context_channel_send_total`, `context_channel_pull_total`, `context_workspace_upsert_total`
- **Audit log**: every `CONTEXT UPSERT/DELETE` and `CONTEXT_SEARCH` emits an `internal.base` audit line with request-id, scope, and key parameters.

## Prerequisites

The internal-context fragments table carries an inline GIN inverted index on `fragment_text` and a vector index on `embedding`. Vector indexes are always available, while the GIN index requires the experimental FE config `enable_experimental_gin`, which defaults to `false`. If this flag is disabled, the bootstrap daemon creates the `__internal_context` database but skips all fragment/index tables — CONTEXT UPSERT then silently drops fragments and every search returns zero hits. To recover, set `enable_experimental_gin` to `true` (`ADMIN SET FRONTEND CONFIG` is picked up within one 60s daemon tick, no FE restart needed) and add it to `fe.conf` so the setting survives a restart.

## Embedding provider configuration

Embedding provider settings (endpoint, model, dimensions, timeout, api_key) are SQL-managed objects, persisted on the FE metadata journal / image — they are not FE configs. Manage them via [`CREATE / ALTER / DROP / SHOW / DESC / SET DEFAULT EMBEDDING PROVIDER`](../sql-reference/sql-statements/cluster-management/embedding/CREATE_EMBEDDING_PROVIDER.md). Every CONTEXT write path resolves the current `DEFAULT EMBEDDING PROVIDER` at request time; if none is set, `CONTEXT UPSERT` and `vector_search` over `query_text` fail with `VECTOR_NOT_READY`.

Example bootstrap (admin only — SYSTEM-level `OPERATE` privilege):

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

The two remaining FE configs that affect the embedding column live in `fe.conf` (or are mutable via `ADMIN SET FRONTEND CONFIG`):

| Config | Default | Purpose |
|---|---|---|
| `context_vector_index_dim` | 1536 | Embedding vector dimension baked into the inline HNSW index when `context_entity_fragments` is first created. Must match the dimension produced by the `DEFAULT EMBEDDING PROVIDER`; mismatches fail embedding writes. Must be set before the first leader boot — `ALTER TABLE ADD INDEX USING VECTOR` is rejected on cloud-native tables. |
| `context_vector_index_metric` | `cosine_similarity` | Distance metric for the HNSW index. One of `cosine_similarity`, `l2_distance`, `inner_product`. Empty omits the vector index slot from the DDL. Captured at first leader boot — changes have no effect after the table exists. |

## Limitations and roadmap

- **Execution ownership**: supported SQL TVFs are now materialized in FE through the same semantic-context services the REST surface uses. The BE `ContextGet` operator remains a schema-compatible fallback stub for introspection and future native operators; it is not the user-facing execution path.
- **Vector search**: standalone `/api/context/vector-search` and fused `CONTEXT_SEARCH` both execute the real vector path. When `query_embedding` is supplied, the query runs even without a configured embedding provider; when only `query_text` is supplied, a configured provider is still required.
- **Inverted-index pushdown**: text search pushes down to the GIN index for single-token `MATCH` queries; multi-token / wildcard patterns still fall back to `LIKE` scans to preserve semantics.
- **Real embedding provider**: register one via `CREATE EMBEDDING PROVIDER ...` and `SET <name> AS DEFAULT EMBEDDING PROVIDER` (see "Embedding provider configuration" above). Without a default provider, `CONTEXT UPSERT` fails per-row because the writer requires a real embedding for the fragments INSERT, and `vector_search` over `query_text` cannot produce fresh query embeddings. Point the endpoint at any OpenAI-compatible `/v1/embeddings` service; `dimensions` in the provider must match `context_vector_index_dim`.

## Embedding compute on BE

Per-row embedding compute runs on the BE side via the scalar function `embedding(text VARCHAR, config JSON) -> ARRAY<FLOAT>`. Bulk inserts and SQL-driven query embeddings parallelize across all BE nodes; the FE leader is no longer a serial bottleneck. The function takes the same OpenAI-compatible `/v1/embeddings` shape as the FE provider:

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

The `config` argument is optional. When it is omitted, the FE resolves the current `DEFAULT EMBEDDING PROVIDER` and injects its config automatically, so a single-argument call is enough once a default provider exists:

```sql
-- Equivalent to embedding('hello world', parse_json('<default provider config>'))
SELECT embedding('hello world');
```

If no `DEFAULT EMBEDDING PROVIDER` is set, the single-argument form fails analysis with a message pointing at `CREATE EMBEDDING PROVIDER` / `SET <name> AS DEFAULT EMBEDDING PROVIDER`. Pass an explicit `config` to embed against a non-default provider.

Internal callers (`ContextWriteExecutor`, `VectorSearchExecutor`) materialize the same shape from the current `DEFAULT EMBEDDING PROVIDER` — `api_key` is omitted entirely when the provider has none (local / self-hosted endpoints that don't require an `Authorization` header).

### API key handling

The API key value lives in FE metadata (journal + image) and is inlined into the BE `embedding(text, config_json)` call site:

- Manage provider credentials via `CREATE / ALTER EMBEDDING PROVIDER` — the `api_key` property travels with the provider object and is persisted on the FE meta journal and image so cluster restarts and upgrades retain the credential.
- `SHOW EMBEDDING PROVIDERS` and `DESC EMBEDDING PROVIDER` mask the key as `******`. The plaintext is only read internally by `EmbeddingConfigJson` / `FeEmbeddingClient` when building the config JSON for the BE-side `embedding(...)` call.
- The audit-log redactor (`SqlCredentialRedactor`) masks any literal `"api_key":"<value>"` it sees in SQL strings.
- All `EMBEDDING PROVIDER` DDL (including `SHOW` / `DESC`) requires SYSTEM-level `OPERATE` privilege.
- Anyone with read access to the FE meta image / BDB journal directory can read provider `api_key` values. Protect those files with filesystem permissions.

If no `DEFAULT EMBEDDING PROVIDER` is set, every write path that needs an embedding fails fast with `VECTOR_NOT_READY`.

The config JSON is parsed by the same `ModelConfig` helper that backs the generic `ai_query(text VARCHAR, config JSON) -> VARCHAR` LLM scalar (opcode 200000, registered alongside `embedding` at 200001). That means `temperature`, `max_tokens`, and `top_p` are accepted in the JSON but ignored on the embedding path — paste-as-is from an `ai_query` config will work, but those fields don't influence `/v1/embeddings` requests.
