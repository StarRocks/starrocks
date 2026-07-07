// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.context;

import com.google.common.collect.ImmutableList;
import com.starrocks.scheduler.history.TableKeeper;

import java.util.List;

/**
 * DDL definitions and {@link TableKeeper} factories for the semantic-context internal tables.
 *
 * <p>The tables live in a hidden database {@code __internal_context} and are bootstrapped on leader
 * promotion by the {@code TableKeeperDaemon}. Every table is a Primary Key model so that version-level
 * immutable history and current-head acceleration can share the same physical engine. Schemas follow
 * the architecture document §7.
 */
public final class ContextInternalTables {

    public static final String DATABASE = "__internal_context";

    public static final String VERSIONS = "context_entity_versions";
    public static final String HEADS = "context_entity_heads";
    public static final String FRAGMENTS = "context_entity_fragments";
    public static final String REFS = "context_entity_refs";
    public static final String COMMITS = "context_commits";
    public static final String WORKSPACE_OBJECTS = "context_workspace_objects";
    public static final String CHANNEL_SUBSCRIPTIONS = "context_channel_subscriptions";
    public static final String TASKS = "context_tasks";

    private static final String VERSIONS_DDL = String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + VERSIONS + " (",
            "  entity_id BIGINT NOT NULL,",
            "  version BIGINT NOT NULL,",
            "  entity_key VARCHAR(512),",
            "  contextbase_id BIGINT NOT NULL,",
            "  collection_id BIGINT NOT NULL,",
            "  collection_type VARCHAR(32) NOT NULL,",
            "  entity_type VARCHAR(32) NOT NULL,",
            "  status VARCHAR(32) NOT NULL,",
            "  title VARCHAR(1024),",
            "  preview VARCHAR(2048) NOT NULL,",
            "  body STRING NOT NULL,",
            "  raw_markdown STRING,",
            "  frontmatter_json JSON,",
            "  source_json JSON,",
            "  confidence DOUBLE NOT NULL,",
            "  body_token_count BIGINT NOT NULL,",
            "  created_time DATETIME NOT NULL,",
            "  updated_time DATETIME NOT NULL,",
            "  commit_time DATETIME NOT NULL,",
            "  snapshot_version BIGINT NOT NULL,",
            "  request_id VARCHAR(128),",
            "  deleted BOOLEAN NOT NULL",
            ")",
            "PRIMARY KEY (entity_id, version)",
            "DISTRIBUTED BY HASH(entity_id) BUCKETS 16",
            "ORDER BY (contextbase_id, collection_id, entity_type, updated_time)",
            "PROPERTIES('replication_num'='1')");

    private static final String HEADS_DDL = String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + HEADS + " (",
            "  entity_id BIGINT NOT NULL,",
            "  entity_key VARCHAR(512),",
            "  contextbase_id BIGINT NOT NULL,",
            "  collection_id BIGINT NOT NULL,",
            "  collection_type VARCHAR(32) NOT NULL,",
            "  entity_type VARCHAR(32) NOT NULL,",
            "  current_version BIGINT NOT NULL,",
            "  current_snapshot_version BIGINT NOT NULL,",
            "  current_preview VARCHAR(2048) NOT NULL,",
            "  current_confidence DOUBLE NOT NULL,",
            "  current_updated_time DATETIME NOT NULL,",
            "  current_deleted BOOLEAN NOT NULL,",
            "  last_ref_version BIGINT,",
            "  last_fragment_version BIGINT",
            ")",
            "PRIMARY KEY (entity_id)",
            "DISTRIBUTED BY HASH(entity_id) BUCKETS 16",
            "ORDER BY (contextbase_id, collection_id, entity_type, current_updated_time)",
            "PROPERTIES('replication_num'='1')");

    // Fragments table carries two retrieval-side indexes, both inlined into CREATE TABLE:
    //
    //   - INVERTED on fragment_text — engages with `MATCH` predicates and lets BE skip rowsets
    //     that don't contain a token. Token parsing uses english analysis which is the safe
    //     default for the agent-base markdown corpus the module targets.
    //
    //   - VECTOR (HNSW) on embedding — built inline at table creation. ALTER TABLE ADD INDEX
    //     for a VECTOR index on a cloud-native (shared-data) table is rejected by
    //     SchemaChangeHandler#processAddIndex (`indexId` is never assigned when
    //     isCloudNativeTableOrMaterializedView() && type == VECTOR), so a deferred bootstrap
    //     can never succeed under shared_data. We bake the index slot into CREATE TABLE with a
    //     fixed metric (cosine_similarity) and dimension; the embedding dimension is governed by
    //     the embedding provider and the write path validates produced vectors against the baked
    //     dimension.
    //
    //     Index type pinned to HNSW: the IVFPQ build path crashes BE in
    //     tenann::FaissIvfPqIndexBuilder::InitIndex() on this branch (SIGSEGV during inline
    //     write and during lake compaction's vector column-group write), so the only stable
    //     option today is HNSW. HNSW is graph-based / non-training, so it has no "train fails
    //     when rows < nlist" failure mode that IVFPQ has.
    //
    // file_bundling = false: in shared_data mode the default storage volume sets file_bundling
    // = true, which makes the lake tablet writer mark bundle segments as skip_vector_index
    // (general_tablet_writer.cpp:289 — bundle segments don't carry .vi files at all, the comment
    // there says "Vector indexes will be built after compaction"). Disabling bundling on this
    // table keeps the HNSW build on the inline write path so ANN queries pick up an index from
    // the first segment, not "eventually after a compaction we don't trigger on small corpora".
    //
    // The FE TextSearchExecutor uses `MATCH` for single-token keyword queries and falls back to
    // `LIKE` for multi-token / wildcard cases so semantics stay grep-compatible while the common
    // keyword path still benefits from index pushdown.
    static String buildFragmentsDdl() {
        StringBuilder indexClauses = new StringBuilder();
        indexClauses.append("  INDEX inv_fragment_text (fragment_text) USING GIN ")
                .append("(\"parser\" = \"english\") COMMENT 'inverted index for context text search'");
        // BITMAP indexes on the denormalized scope columns. These let the BE segment iterator
        // narrow the scan range to the requested contextbase/collection (via _apply_bitmap_index)
        // BEFORE the ANN search, so the vector index runs a scoped (filtered) search instead of a
        // global top-N that a small scope would be filtered out of. Low-cardinality columns
        // (dozens of contextbases / hundreds of collections) are ideal for BITMAP.
        indexClauses.append(",\n")
                .append("  INDEX idx_contextbase_id (contextbase_id) USING BITMAP ")
                .append("COMMENT 'scope pre-filter for vector search'");
        indexClauses.append(",\n")
                .append("  INDEX idx_collection_id (collection_id) USING BITMAP ")
                .append("COMMENT 'scope pre-filter for vector search'");

        // The metric is always cosine_similarity. The embedding dimension is governed by the
        // embedding provider (the write path validates produced vectors against it); 1536 is the
        // shared fragments table's baked dimension and matches the common embedding providers.
        // HNSW tuning: M=16 / efconstruction=40 matches IndexParams defaults (IndexParams.java:63-65).
        int dim = 1536;
        indexClauses.append(",\n")
                .append("  INDEX vec_embedding (embedding) USING VECTOR (")
                .append("\"index_type\" = \"HNSW\", ")
                .append("\"dim\" = \"").append(dim).append("\", ")
                .append("\"metric_type\" = \"cosine_similarity\", ")
                .append("\"M\" = \"16\", ")
                .append("\"efconstruction\" = \"40\"")
                .append(") COMMENT 'HNSW vector index for context ANN search'");

        return String.join("\n",
                "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + FRAGMENTS + " (",
                "  entity_id BIGINT NOT NULL,",
                "  version BIGINT NOT NULL,",
                "  fragment_id BIGINT NOT NULL,",
                "  fragment_kind VARCHAR(32) NOT NULL,",
                "  ordinal INT NOT NULL,",
                "  line_start INT,",
                "  line_end INT,",
                "  fragment_preview VARCHAR(2048),",
                "  fragment_text STRING NOT NULL,",
                "  token_count BIGINT NOT NULL,",
                "  embedding ARRAY<FLOAT> NOT NULL,",
                "  snapshot_version BIGINT NOT NULL,",
                // Denormalized from heads, immutable per entity. Nullable so a missed write path
                // never breaks inserts; the writer always populates them and the recall test guards
                // correctness. Used as a scan-residual scope pre-filter (BITMAP).
                "  contextbase_id BIGINT,",
                "  collection_id BIGINT,",
                indexClauses.toString(),
                ")",
                "PRIMARY KEY (entity_id, version, fragment_id)",
                "DISTRIBUTED BY HASH(entity_id) BUCKETS 16",
                "ORDER BY (entity_id, version, ordinal)",
                "PROPERTIES('replication_num'='1', 'file_bundling'='false')");
    }

    // Refs PK includes snapshot_version so REFERENCE_RESYNC can append a fresh resolution
    // without overwriting the historical row. Read paths pick "the resolution that was active
    // at snapshot X" by picking the MAX(snapshot_version) per (src_entity_id, src_version, ord)
    // that is <= X. Without snapshot_version in the PK, the original DELETE-then-INSERT
    // implementation of REFERENCE_RESYNC silently rewrote history and broke as_of reads.
    private static final String REFS_DDL = String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + REFS + " (",
            "  src_entity_id BIGINT NOT NULL,",
            "  src_version BIGINT NOT NULL,",
            "  ord INT NOT NULL,",
            "  snapshot_version BIGINT NOT NULL,",
            "  dst_entity_id BIGINT NOT NULL,",
            "  ref_kind VARCHAR(32) NOT NULL,",
            "  ref_label VARCHAR(256),",
            // Forward-reference-safe edges (RDF/KG style): an explicit edge always stores the
            // destination's stable key here. dst_entity_id holds the resolved numeric id when the
            // target already exists at write time (fast path); when it does not, dst_entity_id is
            // the sentinel 0 ("unresolved") and traversal resolves dst_entity_key -> heads at read
            // time. NULL for inline/source refs, which always carry a real id.
            "  dst_entity_key VARCHAR(512) NULL",
            ")",
            "PRIMARY KEY (src_entity_id, src_version, ord, snapshot_version)",
            "DISTRIBUTED BY HASH(src_entity_id) BUCKETS 16",
            "ORDER BY (src_entity_id, src_version, dst_entity_id)",
            "PROPERTIES('replication_num'='1')");

    private static final String COMMITS_DDL = String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + COMMITS + " (",
            "  snapshot_version BIGINT NOT NULL,",
            "  contextbase_id BIGINT NOT NULL,",
            "  request_id VARCHAR(128),",
            "  commit_time DATETIME NOT NULL,",
            "  visibility_state VARCHAR(32) NOT NULL,",
            "  primary_ready BOOLEAN NOT NULL,",
            "  refs_ready BOOLEAN NOT NULL,",
            "  fragments_ready BOOLEAN NOT NULL,",
            "  error_message VARCHAR(2048)",
            ")",
            "PRIMARY KEY (snapshot_version)",
            "DISTRIBUTED BY HASH(snapshot_version) BUCKETS 4",
            "ORDER BY (contextbase_id, commit_time)",
            "PROPERTIES('replication_num'='1')");

    private static final String WORKSPACE_OBJECTS_DDL = String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + WORKSPACE_OBJECTS + " (",
            "  workspace_id BIGINT NOT NULL,",
            "  object_id VARCHAR(256) NOT NULL,",
            "  version BIGINT NOT NULL,",
            "  workspace_scope VARCHAR(32),",
            "  object_type VARCHAR(64) NOT NULL,",
            "  payload_json JSON NOT NULL,",
            "  priority DOUBLE NOT NULL,",
            "  ttl_expire_time DATETIME NOT NULL,",
            "  updated_time DATETIME,",
            "  snapshot_version BIGINT NOT NULL,",
            "  deleted BOOLEAN NOT NULL",
            ")",
            "PRIMARY KEY (workspace_id, object_id, version)",
            "DISTRIBUTED BY HASH(workspace_id) BUCKETS 8",
            "ORDER BY (workspace_id, ttl_expire_time)",
            "PROPERTIES('replication_num'='1')");

    private static final String CHANNEL_SUBSCRIPTIONS_DDL =
            buildChannelSubscriptionsCreateSql(CHANNEL_SUBSCRIPTIONS);

    private static final String TASKS_DDL = String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + TASKS + " (",
            "  task_id BIGINT NOT NULL,",
            "  contextbase_id BIGINT NOT NULL,",
            "  task_type VARCHAR(64) NOT NULL,",
            "  state VARCHAR(32) NOT NULL,",
            "  payload_json JSON,",
            "  created_time DATETIME NOT NULL,",
            "  updated_time DATETIME NOT NULL,",
            "  error_message VARCHAR(2048)",
            ")",
            "PRIMARY KEY (task_id)",
            "DISTRIBUTED BY HASH(task_id) BUCKETS 4",
            "ORDER BY (contextbase_id, updated_time)",
            "PROPERTIES('replication_num'='1')");

    private ContextInternalTables() {
    }

    static String buildChannelSubscriptionsCreateSql(String tableName) {
        return String.join("\n",
                "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + tableName + " (",
                "  subscription_id BIGINT NOT NULL,",
                "  subscriber VARCHAR(256) NOT NULL,",
                "  contextbase_id BIGINT NOT NULL,",
                "  collection_id BIGINT NOT NULL,",
                "  pattern VARCHAR(512) NOT NULL,",
                "  subscription_type VARCHAR(32) NOT NULL,",
                "  last_cursor_snapshot BIGINT,",
                "  created_time DATETIME NOT NULL,",
                "  deleted BOOLEAN NOT NULL",
                ")",
                "PRIMARY KEY (subscription_id)",
                "DISTRIBUTED BY HASH(subscription_id) BUCKETS 4",
                "ORDER BY (subscriber, contextbase_id, collection_id)",
                "PROPERTIES('replication_num'='1')");
    }

    public static List<TableKeeper> createKeepers() {
        return ImmutableList.of(
                new TableKeeper(DATABASE, VERSIONS, VERSIONS_DDL, null),
                new TableKeeper(DATABASE, HEADS, HEADS_DDL, null),
                new TableKeeper(DATABASE, FRAGMENTS, buildFragmentsDdl(), null),
                new TableKeeper(DATABASE, REFS, REFS_DDL, null),
                new TableKeeper(DATABASE, COMMITS, COMMITS_DDL, null),
                new TableKeeper(DATABASE, WORKSPACE_OBJECTS, WORKSPACE_OBJECTS_DDL, null),
                new TableKeeper(DATABASE, CHANNEL_SUBSCRIPTIONS, CHANNEL_SUBSCRIPTIONS_DDL, null),
                new TableKeeper(DATABASE, TASKS, TASKS_DDL, null));
    }
}
