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

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.starrocks.context.markdown.MarkdownExtractor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Read-side counterpart to {@link ContextWriteExecutor}. Builds SELECT statements against the
 * semantic-context internal tables and returns the decoded JSON rows. Used by REST actions and by
 * the M3 retrieval planner (which rewrites context TVFs into these same queries before the BE
 * operators for ranking/fusion are finished).
 *
 * <p>Keeping the SQL construction in one place means the write schema and read schema stay in sync
 * (e.g. both sides agree on column order for {@code context_entity_versions}); it also keeps
 * {@code SimpleExecutor.getRepoExecutor()} usage uniform so the audit log lists every internal read.
 */
public class ContextReadExecutor {

    private static final Logger LOG = LogManager.getLogger(ContextReadExecutor.class);

    /**
     * Disclosure level per arch doc §11. {@code preview} returns just heads + preview; {@code standard}
     * returns body + (caller-side) link previews; {@code deep} additionally fetches one layer of
     * linked-page bodies. Default is {@link DisclosureLevel#STANDARD}.
     */
    public enum DisclosureLevel {
        PREVIEW,
        STANDARD,
        DEEP;

        public static DisclosureLevel parse(String s) {
            if (s == null || s.isEmpty()) {
                return STANDARD;
            }
            try {
                return DisclosureLevel.valueOf(s.toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return STANDARD;
            }
        }
    }

    public static final class EntityMeta {
        public final long entityId;
        public final String entityKey;
        public final String entityType;
        public final long version;
        public final long snapshotVersion;
        public final String preview;
        public final double confidence;
        public final String title;
        // Raw frontmatter JSON for the version row. Search-response handlers surface this
        // verbatim so callers (the bench, downstream agents) can recover the original ingest
        // path without a follow-up /api/context/get round-trip per hit.
        public final String frontmatterJson;

        public EntityMeta(long entityId, String entityKey, String entityType, long version, long snapshotVersion,
                          String preview, double confidence, String title,
                          String frontmatterJson) {
            this.entityId = entityId;
            this.entityKey = entityKey;
            this.entityType = entityType;
            this.version = version;
            this.snapshotVersion = snapshotVersion;
            this.preview = preview;
            this.confidence = confidence;
            this.title = title;
            this.frontmatterJson = frontmatterJson;
        }
    }

    public static final class VersionRow {
        public final long entityId;
        public final long version;
        public final String entityKey;
        public final String entityType;
        public final long contextBaseId;
        public final long collectionId;
        public final String title;
        public final String preview;
        public final String body;
        public final String rawMarkdown;
        public final String frontmatterJson;
        public final String sourceJson;
        public final double confidence;
        public final String createdTime;
        public final String updatedTime;
        public final String commitTime;
        public final long snapshotVersion;
        public final boolean deleted;

        public VersionRow(long entityId, long version, String entityKey, String entityType,
                          long contextBaseId, long collectionId, String title, String preview,
                          String body, String rawMarkdown, String frontmatterJson, String sourceJson,
                          double confidence, String createdTime, String updatedTime, String commitTime,
                          long snapshotVersion, boolean deleted) {
            this.entityId = entityId;
            this.version = version;
            this.entityKey = entityKey;
            this.entityType = entityType;
            this.contextBaseId = contextBaseId;
            this.collectionId = collectionId;
            this.title = title;
            this.preview = preview;
            this.body = body;
            this.rawMarkdown = rawMarkdown;
            this.frontmatterJson = frontmatterJson;
            this.sourceJson = sourceJson;
            this.confidence = confidence;
            this.createdTime = createdTime;
            this.updatedTime = updatedTime;
            this.commitTime = commitTime;
            this.snapshotVersion = snapshotVersion;
            this.deleted = deleted;
        }

        public String effectiveRawMarkdown() {
            return MarkdownExtractor.canonicalizeRawMarkdown(rawMarkdown, body, frontmatterJson, sourceJson);
        }
    }

    /**
     * Read the current head row plus the body for an entity by numeric id. Returns the decoded
     * rows as a JSON array (each element is a {@code {"data": [...]}} row).
     */
    public JsonArray getCurrentById(long entityId) {
        return getCurrentById(entityId, DisclosureLevel.STANDARD);
    }

    /**
     * Disclosure-aware variant. {@link DisclosureLevel#PREVIEW} skips the version-table join so
     * the response carries no body. {@link DisclosureLevel#STANDARD} and {@link DisclosureLevel#DEEP}
     * both include the body; {@code DEEP}'s extra hop is fetched by the caller via
     * {@link #getNeighbourPreviews(long, int)} or {@link #getNeighbourBodies(long, int)}.
     */
    public JsonArray getCurrentById(long entityId, DisclosureLevel level) {
        if (level == DisclosureLevel.PREVIEW) {
            String sql = String.format(
                    "SELECT entity_id, current_version, entity_key, entity_type, current_preview, "
                            + "current_snapshot_version, current_deleted "
                            + "FROM %s.%s WHERE entity_id = %d",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS, entityId);
            return runQuery(sql);
        }
        String sql = String.format(
                "SELECT h.entity_id, h.current_version, h.entity_key, h.entity_type, h.current_preview, "
                        + "h.current_snapshot_version, h.current_deleted, v.title, v.body, "
                        + "v.confidence, v.updated_time "
                        + "FROM %s.%s h JOIN %s.%s v "
                        + "ON h.entity_id = v.entity_id AND h.current_version = v.version "
                        + "WHERE h.entity_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                entityId);
        return runQuery(sql);
    }

    /**
     * For {@code STANDARD} disclosure (or {@code DEEP}'s outer ring): fetch the preview rows for
     * every entity referenced by the seed entity's current version. Returns at most
     * {@code maxNeighbours} rows in stable id order so callers can attach them next to the body.
     */
    public JsonArray getNeighbourPreviews(long seedEntityId, int maxNeighbours) {
        int limit = maxNeighbours > 0 ? maxNeighbours : 32;
        String sql = String.format(
                "SELECT h.entity_id, h.entity_key, h.current_preview, h.current_snapshot_version "
                        + "FROM %s.%s h "
                        + "JOIN ("
                        + "  SELECT DISTINCT r.dst_entity_id "
                        + "  FROM %s.%s r "
                        + "  JOIN %s.%s seed ON r.src_entity_id = seed.entity_id AND r.src_version = seed.current_version "
                        + "  WHERE seed.entity_id = %d"
                        + ") t ON h.entity_id = t.dst_entity_id "
                        + "ORDER BY h.entity_id LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                ContextInternalTables.DATABASE, ContextInternalTables.REFS,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                seedEntityId, limit);
        return runQuery(sql);
    }

    /**
     * For {@code DEEP} disclosure: fetch the bodies of one hop of neighbours. Token-budget caps
     * are the caller's responsibility — this returns whole rows.
     */
    public JsonArray getNeighbourBodies(long seedEntityId, int maxNeighbours) {
        int limit = maxNeighbours > 0 ? maxNeighbours : 8;
        String sql = String.format(
                "SELECT v.entity_id, v.version, v.entity_key, v.title, v.body, v.snapshot_version "
                        + "FROM %s.%s v "
                        + "JOIN ("
                        + "  SELECT DISTINCT r.dst_entity_id AS dst, h.current_version AS dst_v "
                        + "  FROM %s.%s r "
                        + "  JOIN %s.%s seed ON r.src_entity_id = seed.entity_id AND r.src_version = seed.current_version "
                        + "  JOIN %s.%s h ON h.entity_id = r.dst_entity_id "
                        + "  WHERE seed.entity_id = %d AND h.current_deleted = false"
                        + ") t ON v.entity_id = t.dst AND v.version = t.dst_v "
                        + "ORDER BY v.entity_id LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                ContextInternalTables.DATABASE, ContextInternalTables.REFS,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                seedEntityId, limit);
        return runQuery(sql);
    }

    /**
     * Resolve and read the current head row by string entity key, scoped to a specific
     * {@code (contextBaseId, collectionId)}. Either may be null when the caller doesn't have one
     * yet, but the design's invariant (entity_key unique per collection) means a global lookup is
     * never the right semantics — callers should always pass a contextbase at minimum.
     */
    public JsonArray getCurrentByKey(String entityKey, Long contextBaseId, Long collectionId) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT h.entity_id, h.current_version, h.entity_key, h.entity_type, h.current_preview, "
                + "h.current_snapshot_version, h.current_deleted, v.title, v.body, "
                + "v.confidence, v.updated_time ");
        sql.append("FROM ").append(ContextInternalTables.DATABASE).append('.')
                .append(ContextInternalTables.HEADS).append(" h ");
        sql.append("JOIN ").append(ContextInternalTables.DATABASE).append('.')
                .append(ContextInternalTables.VERSIONS).append(" v ");
        sql.append("ON h.entity_id = v.entity_id AND h.current_version = v.version ");
        sql.append("WHERE h.entity_key = '").append(escapeSql(entityKey)).append('\'');
        if (contextBaseId != null) {
            sql.append(" AND h.contextbase_id = ").append(contextBaseId);
        }
        if (collectionId != null) {
            sql.append(" AND h.collection_id = ").append(collectionId);
        }
        return runQuery(sql.toString());
    }

    /**
     * Read a specific version of an entity via PK point lookup on {@code context_entity_versions}.
     */
    public JsonArray getExactVersion(long entityId, long version) {
        String sql = String.format(
                "SELECT entity_id, version, entity_key, entity_type, preview, body, snapshot_version, "
                        + "deleted, 'READY', confidence, updated_time "
                        + "FROM %s.%s WHERE entity_id = %d AND version = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId, version);
        return runQuery(sql);
    }

    /**
     * Full version timeline for an entity (all versions, descending), capped at the FE-side
     * maximum (see {@link com.starrocks.common.Config#context_entity_history_max_rows}).
     */
    public JsonArray getHistory(long entityId) {
        return getHistory(entityId, com.starrocks.common.Config.context_entity_history_max_rows);
    }

    /**
     * Full version timeline for an entity (all versions, descending). Capped to at most
     * {@link com.starrocks.common.Config#context_entity_history_max_rows} rows to keep a
     * frequently-versioned entity from materializing thousands of rows as ValuesRelation Expr
     * nodes on the FE. A positive caller {@code limit} is honoured only up to that maximum; a
     * non-positive {@code limit} uses the maximum.
     */
    public JsonArray getHistory(long entityId, int limit) {
        int effective = effectiveHistoryLimit(limit);
        String sql = String.format(
                "SELECT entity_id, version, snapshot_version, updated_time, deleted, preview, confidence "
                        + "FROM %s.%s WHERE entity_id = %d ORDER BY version DESC LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId, effective);
        return runQuery(sql);
    }

    /**
     * Resolve the effective row cap for a history read. The Config value is a hard ceiling: a
     * non-positive caller {@code limit} falls back to it, and a larger positive caller limit is
     * clamped down to it. Package-private for shape testing.
     */
    static int effectiveHistoryLimit(int limit) {
        int max = com.starrocks.common.Config.context_entity_history_max_rows;
        int requested = limit > 0 ? limit : max;
        return Math.min(requested, max);
    }

    /**
     * Batch metadata loader used by retrieval actions and TVF materialization. Preserves the input
     * entity-id order in the returned map.
     */
    public Map<Long, EntityMeta> loadEntityMetadata(Collection<Long> entityIds, long snapshotFence) {
        Map<Long, EntityMeta> ordered = new LinkedHashMap<>();
        if (entityIds == null || entityIds.isEmpty()) {
            return ordered;
        }
        StringBuilder inList = new StringBuilder();
        boolean first = true;
        for (Long entityId : entityIds) {
            if (entityId == null) {
                continue;
            }
            if (!first) {
                inList.append(',');
            }
            inList.append(entityId);
            first = false;
        }
        if (inList.length() == 0) {
            return ordered;
        }

        String sql;
        if (snapshotFence < 0) {
            sql = String.format(
                    "SELECT h.entity_id, h.entity_key, h.entity_type, h.current_version, "
                            + "h.current_snapshot_version, h.current_preview, "
                            + "h.current_confidence, v.title, v.frontmatter_json "
                            + "FROM %s.%s h LEFT JOIN %s.%s v "
                            + "ON h.entity_id = v.entity_id AND h.current_version = v.version "
                            + "WHERE h.entity_id IN (%s) AND h.current_deleted = false "
                            + "ORDER BY h.entity_id",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                    inList);
        } else {
            sql = String.format(
                    "SELECT v.entity_id, v.entity_key, v.entity_type, v.version, v.snapshot_version, "
                            + "v.preview, "
                            + "v.confidence, v.title, v.frontmatter_json "
                            + "FROM %s.%s v JOIN ("
                            + "  SELECT entity_id, MAX(version) AS av FROM %s.%s "
                            + "  WHERE entity_id IN (%s) AND snapshot_version <= %d GROUP BY entity_id"
                            + ") av ON av.entity_id = v.entity_id AND av.av = v.version "
                            + "LEFT JOIN %s.%s h ON h.entity_id = v.entity_id "
                            + "WHERE v.deleted = false ORDER BY v.entity_id",
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                    inList, snapshotFence,
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS);
        }

        Map<Long, EntityMeta> loaded = new LinkedHashMap<>();
        JsonArray rows = runQuery(sql);
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            long entityId = data.get(0).getAsLong();
            // frontmatter_json is a StarRocks JSON column; the executor's wire format may render
            // it as a JsonObject/JsonArray rather than a primitive string. Calling getAsString()
            // on a non-primitive throws UnsupportedOperationException, so route non-primitive
            // values through their wire form (toString()).
            String frontmatterJson = null;
            if (data.size() > 8 && !data.get(8).isJsonNull()) {
                JsonElement fmElem = data.get(8);
                frontmatterJson = fmElem.isJsonPrimitive() ? fmElem.getAsString() : fmElem.toString();
            }
            loaded.put(entityId, new EntityMeta(
                    entityId,
                    data.get(1).isJsonNull() ? null : data.get(1).getAsString(),
                    data.get(2).isJsonNull() ? null : data.get(2).getAsString(),
                    data.get(3).isJsonNull() ? 0L : data.get(3).getAsLong(),
                    data.get(4).isJsonNull() ? 0L : data.get(4).getAsLong(),
                    data.get(5).isJsonNull() ? null : data.get(5).getAsString(),
                    data.get(6).isJsonNull() ? 0.0 : data.get(6).getAsDouble(),
                    data.get(7).isJsonNull() ? null : data.get(7).getAsString(),
                    frontmatterJson));
        }
        for (Long entityId : entityIds) {
            if (entityId != null && loaded.containsKey(entityId)) {
                ordered.put(entityId, loaded.get(entityId));
            }
        }
        return ordered;
    }

    /**
     * Read every entity in a collection as full version rows. When {@code snapshotFence} is
     * {@code -1}, returns the current head version; when {@code >= 0}, returns the visible version
     * whose {@code snapshot_version} is the largest value not exceeding the fence.
     *
     * <p>Back-compat shim: delegates to the paginated overload with {@code offset=0} and no cursor.
     */
    public JsonArray readCollection(long collectionId, long snapshotFence, int limit) {
        return readCollection(collectionId, snapshotFence, limit, 0, -1L);
    }

    /**
     * Paginated variant of {@link #readCollection(long, long, int)}.
     *
     * <p>Ordering: every page is sorted by the entity's primary key {@code entity_id ASC}. This is
     * (a) a total order — {@code entity_id} is allocated monotonically and uniquely by
     * {@link com.starrocks.context.allocator.ContextVersionAllocator}, so the previous
     * {@code current_snapshot_version DESC} bug (1000-row commits sharing a fence had
     * non-deterministic tiebreak across BEs, causing OFFSET to duplicate / skip rows) is gone;
     * and (b) PK-friendly — both heads and versions are PK tables keyed on {@code entity_id}, so
     * a cursor predicate {@code entity_id > afterEntityId} is a range scan with the LIMIT pushed
     * to OlapScan.
     *
     * <p>{@code afterEntityId >= 0} takes precedence over {@code offset} and yields keyset
     * pagination — O(log N + N) per page, stable under concurrent upserts (new ids are appended
     * past any previously-returned cursor), and the right primitive for walking large collections.
     * {@code offset > 0} still works (correctness restored), but cost grows linearly with offset.
     */
    public JsonArray readCollection(long collectionId, long snapshotFence, int limit, int offset,
                                    long afterEntityId) {
        int effectiveLimit = limit > 0 ? limit : 1000;
        int effectiveOffset = Math.max(0, offset);
        String sql = buildReadCollectionSql(collectionId, snapshotFence, effectiveLimit,
                effectiveOffset, afterEntityId);
        return runQuery(sql);
    }

    static String buildReadCollectionSql(long collectionId, long snapshotFence, int limit,
                                         int offset, long afterEntityId) {
        if (snapshotFence < 0) {
            return String.format(
                    "SELECT v.entity_id, v.version, v.entity_key, v.entity_type, v.contextbase_id, v.collection_id, "
                            + "v.title, v.preview, v.body, v.raw_markdown, v.frontmatter_json, v.source_json, "
                            + "v.confidence, v.created_time, v.updated_time, v.commit_time, v.snapshot_version, "
                            + "v.deleted "
                            + "FROM %s.%s h JOIN %s.%s v "
                            + "ON h.entity_id = v.entity_id AND h.current_version = v.version "
                            + "WHERE h.collection_id = %d%s ORDER BY h.entity_id ASC LIMIT %d%s",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                    collectionId, cursorPredicate("h.entity_id", afterEntityId), limit,
                    offsetClause(offset, afterEntityId));
        }
        return String.format(
                "SELECT v.entity_id, v.version, v.entity_key, v.entity_type, v.contextbase_id, v.collection_id, "
                        + "v.title, v.preview, v.body, v.raw_markdown, v.frontmatter_json, v.source_json, "
                        + "v.confidence, v.created_time, v.updated_time, v.commit_time, v.snapshot_version, "
                        + "v.deleted "
                        + "FROM %s.%s v "
                        + "JOIN ("
                        + "  SELECT entity_id, MAX(version) AS max_version "
                        + "  FROM %s.%s "
                        + "  WHERE collection_id = %d AND snapshot_version <= %d "
                        + "  GROUP BY entity_id"
                        + ") t ON v.entity_id = t.entity_id AND v.version = t.max_version "
                        + "LEFT JOIN %s.%s h ON h.entity_id = v.entity_id "
                        + "WHERE v.collection_id = %d%s ORDER BY v.entity_id ASC LIMIT %d%s",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                collectionId, snapshotFence,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                collectionId, cursorPredicate("v.entity_id", afterEntityId), limit,
                offsetClause(offset, afterEntityId));
    }

    /**
     * Read every entity in a contextbase as full version rows. Uses the same schema/order as
     * {@link #readCollection(long, long, int)} so downstream decoders can share the mapping.
     */
    public JsonArray readContextBase(long contextBaseId, long snapshotFence, int limit) {
        return readContextBase(contextBaseId, snapshotFence, limit, 0, -1L);
    }

    /**
     * Paginated variant of {@link #readContextBase(long, long, int)}. Same pagination contract as
     * {@link #readCollection(long, long, int, int, long)}: cursor takes precedence over offset,
     * both walk over a stable {@code entity_id ASC} total order.
     */
    public JsonArray readContextBase(long contextBaseId, long snapshotFence, int limit, int offset,
                                     long afterEntityId) {
        int effectiveLimit = limit > 0 ? limit : 2000;
        int effectiveOffset = Math.max(0, offset);
        String sql = buildReadContextBaseSql(contextBaseId, snapshotFence, effectiveLimit,
                effectiveOffset, afterEntityId);
        return runQuery(sql);
    }

    static String buildReadContextBaseSql(long contextBaseId, long snapshotFence, int limit,
                                          int offset, long afterEntityId) {
        if (snapshotFence < 0) {
            return String.format(
                    "SELECT v.entity_id, v.version, v.entity_key, v.entity_type, v.contextbase_id, v.collection_id, "
                            + "v.title, v.preview, v.body, v.raw_markdown, v.frontmatter_json, v.source_json, "
                            + "v.confidence, v.created_time, v.updated_time, v.commit_time, v.snapshot_version, "
                            + "v.deleted "
                            + "FROM %s.%s h JOIN %s.%s v "
                            + "ON h.entity_id = v.entity_id AND h.current_version = v.version "
                            + "WHERE h.contextbase_id = %d%s ORDER BY h.entity_id ASC LIMIT %d%s",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                    contextBaseId, cursorPredicate("h.entity_id", afterEntityId), limit,
                    offsetClause(offset, afterEntityId));
        }
        return String.format(
                "SELECT v.entity_id, v.version, v.entity_key, v.entity_type, v.contextbase_id, v.collection_id, "
                        + "v.title, v.preview, v.body, v.raw_markdown, v.frontmatter_json, v.source_json, "
                        + "v.confidence, v.created_time, v.updated_time, v.commit_time, v.snapshot_version, "
                        + "v.deleted "
                        + "FROM %s.%s v "
                        + "JOIN ("
                        + "  SELECT entity_id, MAX(version) AS max_version "
                        + "  FROM %s.%s "
                        + "  WHERE contextbase_id = %d AND snapshot_version <= %d "
                        + "  GROUP BY entity_id"
                        + ") t ON v.entity_id = t.entity_id AND v.version = t.max_version "
                        + "LEFT JOIN %s.%s h ON h.entity_id = v.entity_id "
                        + "WHERE v.contextbase_id = %d%s ORDER BY v.entity_id ASC LIMIT %d%s",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                contextBaseId, snapshotFence,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                contextBaseId, cursorPredicate("v.entity_id", afterEntityId), limit,
                offsetClause(offset, afterEntityId));
    }

    private static String cursorPredicate(String entityIdColumn, long afterEntityId) {
        return afterEntityId >= 0 ? " AND " + entityIdColumn + " > " + afterEntityId : "";
    }

    private static String offsetClause(int offset, long afterEntityId) {
        // Cursor pagination supersedes offset — emitting both would compound the skip semantics.
        return (afterEntityId < 0 && offset > 0) ? " OFFSET " + offset : "";
    }

    /**
     * Count helpers for {@code SHOW CONTEXT STATUS}.
     */
    public long countRows(String tableName) {
        return countWithFilter(tableName, null);
    }

    /**
     * COUNT(*) over a single internal table with an optional WHERE clause. Used by the stats
     * endpoint for per-contextbase counts. Returns -1 when the underlying SELECT throws (table
     * not yet materialized) so callers can render "n/a" instead of an opaque error.
     */
    public long countWithFilter(String tableName, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ");
        sql.append(ContextInternalTables.DATABASE).append('.').append(tableName);
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        return runScalarLong(sql.toString());
    }

    /**
     * Per-contextbase {@code COUNT(*)} of {@link ContextInternalTables#FRAGMENTS}. The fragments
     * table doesn't carry {@code contextbase_id} directly; we join through
     * {@link ContextInternalTables#HEADS} to scope the count.
     */
    public long countFragmentsForContextBase(long contextBaseId) {
        String sql = String.format(
                "SELECT COUNT(*) FROM %s.%s f JOIN %s.%s h "
                        + "ON h.entity_id = f.entity_id WHERE h.contextbase_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.FRAGMENTS,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, contextBaseId);
        return runScalarLong(sql);
    }

    /**
     * Per-contextbase {@code COUNT(*)} of {@link ContextInternalTables#REFS}. Same shape as
     * {@link #countFragmentsForContextBase(long)} — refs are scoped via the source entity's heads
     * row, since the refs table only carries {@code src_entity_id}.
     */
    public long countRefsForContextBase(long contextBaseId) {
        String sql = String.format(
                "SELECT COUNT(*) FROM %s.%s r JOIN %s.%s h "
                        + "ON h.entity_id = r.src_entity_id WHERE h.contextbase_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.REFS,
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, contextBaseId);
        return runScalarLong(sql);
    }

    public String maxUpdatedTimeForContextBase(long contextBaseId) {
        String sql = String.format(
                "SELECT MAX(current_updated_time) FROM %s.%s WHERE contextbase_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, contextBaseId);
        return runScalarString(sql);
    }

    public String maxUpdatedTimeForCollection(long collectionId) {
        String sql = String.format(
                "SELECT MAX(current_updated_time) FROM %s.%s WHERE collection_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, collectionId);
        return runScalarString(sql);
    }

    public long countEntitiesForCollection(long collectionId) {
        return countWithFilter(ContextInternalTables.HEADS,
                "collection_id = " + collectionId + " AND current_deleted = false");
    }

    public long countEntitiesForContextBase(long contextBaseId) {
        return countWithFilter(ContextInternalTables.HEADS,
                "contextbase_id = " + contextBaseId + " AND current_deleted = false");
    }

    /**
     * Bulk variant for {@link #countEntitiesForContextBase}: one GROUP BY query for all supplied
     * base ids, returning a map keyed by contextbase_id. Used by list endpoints to avoid the
     * O(N) round-trips that the per-base helper produces when called in a loop. Missing entries
     * indicate "zero entities" for that base.
     */
    public java.util.Map<Long, Long> bulkCountEntitiesForContextBases(java.util.Collection<Long> baseIds) {
        java.util.Map<Long, Long> out = new java.util.HashMap<>();
        if (baseIds == null || baseIds.isEmpty()) {
            return out;
        }
        StringBuilder inList = new StringBuilder();
        boolean first = true;
        for (Long id : baseIds) {
            if (!first) {
                inList.append(',');
            }
            inList.append(id);
            first = false;
        }
        String sql = String.format(
                "SELECT contextbase_id, COUNT(*) FROM %s.%s "
                        + "WHERE contextbase_id IN (%s) AND current_deleted = false "
                        + "GROUP BY contextbase_id",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, inList);
        try {
            for (JsonElement row : ContextSqlSupport.executeDql(sql)) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data.size() >= 2 && !data.get(0).isJsonNull() && !data.get(1).isJsonNull()) {
                    out.put(data.get(0).getAsLong(), data.get(1).getAsLong());
                }
            }
        } catch (Exception ignored) {
            // Heads not materialized yet — caller falls back to "no count available".
        }
        return out;
    }

    /**
     * Bulk variant for {@link #maxUpdatedTimeForContextBase}: one GROUP BY query, returning a
     * map keyed by contextbase_id. Missing entries indicate "no rows" for that base.
     */
    public java.util.Map<Long, String> bulkMaxUpdatedTimeForContextBases(java.util.Collection<Long> baseIds) {
        java.util.Map<Long, String> out = new java.util.HashMap<>();
        if (baseIds == null || baseIds.isEmpty()) {
            return out;
        }
        StringBuilder inList = new StringBuilder();
        boolean first = true;
        for (Long id : baseIds) {
            if (!first) {
                inList.append(',');
            }
            inList.append(id);
            first = false;
        }
        String sql = String.format(
                "SELECT contextbase_id, MAX(current_updated_time) FROM %s.%s "
                        + "WHERE contextbase_id IN (%s) GROUP BY contextbase_id",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, inList);
        try {
            for (JsonElement row : ContextSqlSupport.executeDql(sql)) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data.size() >= 2 && !data.get(0).isJsonNull() && !data.get(1).isJsonNull()) {
                    out.put(data.get(0).getAsLong(), data.get(1).getAsString());
                }
            }
        } catch (Exception ignored) {
        }
        return out;
    }

    private long runScalarLong(String sql) {
        try {
            for (JsonElement row : ContextSqlSupport.executeDql(sql)) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data.size() > 0 && !data.get(0).isJsonNull()) {
                    return data.get(0).getAsLong();
                }
            }
        } catch (Exception e) {
            return -1L;
        }
        return 0L;
    }

    public String runScalarString(String sql) {
        try {
            for (JsonElement row : ContextSqlSupport.executeDql(sql)) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data.size() > 0 && !data.get(0).isJsonNull()) {
                    return data.get(0).getAsString();
                }
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    /**
     * Resolve {@code entity_key} to {@code entity_id} via the heads table, scoped to a specific
     * {@code (contextBaseId, collectionId)} pair. {@code collectionId} may be null when the caller
     * only needs contextbase scoping. Returns {@code -1L} if not found in that scope.
     *
     * <p>The unscoped overload below is intentionally narrow: it exists for back-compat callers
     * that have no scope on hand. New callers should always pass a scope so a key collision across
     * collections / bases can't silently route a write to the wrong entity (the bug surfaced in
     * the 2026-04-26 architecture review).
     */
    public long resolveEntityIdByKey(String entityKey, Long contextBaseId, Long collectionId) {
        if (Strings.isNullOrEmpty(entityKey)) {
            return -1L;
        }
        StringBuilder sql = new StringBuilder("SELECT entity_id FROM ");
        sql.append(ContextInternalTables.DATABASE).append('.').append(ContextInternalTables.HEADS)
                .append(" WHERE entity_key = '").append(escapeSql(entityKey)).append('\'');
        if (contextBaseId != null) {
            sql.append(" AND contextbase_id = ").append(contextBaseId);
        }
        if (collectionId != null) {
            sql.append(" AND collection_id = ").append(collectionId);
        }
        sql.append(" LIMIT 1");
        JsonArray rows = runQuery(sql.toString());
        if (rows.size() == 0) {
            return -1L;
        }
        JsonArray data = rows.get(0).getAsJsonObject().getAsJsonArray("data");
        if (data.size() == 0) {
            return -1L;
        }
        return data.get(0).getAsLong();
    }

    /**
     * Unscoped variant — kept for paths that have no scope yet (e.g. the entity-key argument of an
     * unscoped REST GET). Prefer {@link #resolveEntityIdByKey(String, Long, Long)} whenever the
     * caller already knows the contextbase / collection.
     */
    public long resolveEntityIdByKey(String entityKey) {
        return resolveEntityIdByKey(entityKey, null, null);
    }

    /**
     * Bulk variant of {@link #resolveEntityIdByKey(String, Long, Long)}. Used by the batched
     * upsert path to dedup hundreds of {@code entity_key}s into one round-trip instead of N. The
     * scope is identical to the single-key form: {@code contextBaseId} is required, and
     * {@code collectionId} is optional ({@code null} means "anywhere in the contextbase" — the
     * shape edge resolution needs since edges may cross collections within the same base).
     *
     * <p>Returned map only contains keys that resolved; missing keys are absent (caller treats
     * them as "no existing entity, allocate fresh").
     */
    public Map<String, Long> resolveEntityIdsByKeys(java.util.Collection<String> entityKeys,
                                                    long contextBaseId, Long collectionId) {
        Map<String, Long> out = new HashMap<>();
        if (entityKeys == null || entityKeys.isEmpty()) {
            return out;
        }
        // Dedup + skip empties up front so the IN list stays compact even when the caller passed
        // a List with duplicates (typical from per-row args lists in batch upsert).
        java.util.Set<String> distinct = new java.util.LinkedHashSet<>();
        for (String k : entityKeys) {
            if (!Strings.isNullOrEmpty(k)) {
                distinct.add(k);
            }
        }
        if (distinct.isEmpty()) {
            return out;
        }
        StringBuilder sql = new StringBuilder("SELECT entity_id, entity_key FROM ");
        sql.append(ContextInternalTables.DATABASE).append('.').append(ContextInternalTables.HEADS)
                .append(" WHERE contextbase_id = ").append(contextBaseId);
        if (collectionId != null) {
            sql.append(" AND collection_id = ").append(collectionId);
        }
        sql.append(" AND entity_key IN (");
        boolean first = true;
        for (String k : distinct) {
            if (!first) {
                sql.append(',');
            }
            sql.append('\'').append(escapeSql(k)).append('\'');
            first = false;
        }
        sql.append(')');
        JsonArray rows = runQuery(sql.toString());
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data.size() < 2 || data.get(0).isJsonNull() || data.get(1).isJsonNull()) {
                continue;
            }
            out.put(data.get(1).getAsString(), data.get(0).getAsLong());
        }
        return out;
    }

    /**
     * Live-entity variant of {@link #resolveEntityIdsByKeys(java.util.Collection, long, Long)},
     * used by the markdown ref-resolution path. The reference-extraction path must distinguish a
     * tombstoned target ({@code current_deleted = true}) from a live one — pointing graph edges
     * at a soft-deleted entity would surface ghost rows in {@code graph_expand} / search results,
     * since every read path elsewhere in the module filters tombstones out. So this variant adds
     * {@code AND current_deleted = false} to the IN-list scan; a tombstoned key returns absent.
     *
     * <p>Kept separate from {@link #resolveEntityIdsByKeys} on purpose: that method is used by
     * the entity_key REUSE path in {@code ContextWriteExecutor.upsertBatch} (an UPSERT with the
     * same entity_key as a tombstoned entity should resurrect that entity, not allocate a fresh
     * id) and must continue to find tombstoned entities. The ref-resolution and entity-reuse
     * paths have opposite preferences about soft-deleted rows, so they need distinct helpers.
     */
    public Map<String, Long> resolveLiveEntityIdsByKeys(java.util.Collection<String> entityKeys,
                                                        long contextBaseId, Long collectionId) {
        Map<String, Long> out = new HashMap<>();
        if (entityKeys == null || entityKeys.isEmpty()) {
            return out;
        }
        java.util.Set<String> distinct = new java.util.LinkedHashSet<>();
        for (String k : entityKeys) {
            if (!Strings.isNullOrEmpty(k)) {
                distinct.add(k);
            }
        }
        if (distinct.isEmpty()) {
            return out;
        }
        StringBuilder sql = new StringBuilder("SELECT entity_id, entity_key FROM ");
        sql.append(ContextInternalTables.DATABASE).append('.').append(ContextInternalTables.HEADS)
                .append(" WHERE contextbase_id = ").append(contextBaseId)
                .append(" AND current_deleted = false");
        if (collectionId != null) {
            sql.append(" AND collection_id = ").append(collectionId);
        }
        sql.append(" AND entity_key IN (");
        boolean first = true;
        for (String k : distinct) {
            if (!first) {
                sql.append(',');
            }
            sql.append('\'').append(escapeSql(k)).append('\'');
            first = false;
        }
        sql.append(')');
        JsonArray rows = runQuery(sql.toString());
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data.size() < 2 || data.get(0).isJsonNull() || data.get(1).isJsonNull()) {
                continue;
            }
            out.put(data.get(1).getAsString(), data.get(0).getAsLong());
        }
        return out;
    }

    /**
     * Bulk loader for {@code created_time} on each entity's earliest version row. The single-row
     * upsert path looks this up to preserve the original creation timestamp when bumping a
     * version; in batch we collapse N point lookups into one IN-list scan.
     */
    public Map<Long, String> loadCreatedTimes(java.util.Collection<Long> entityIds) {
        Map<Long, String> out = new HashMap<>();
        if (entityIds == null || entityIds.isEmpty()) {
            return out;
        }
        java.util.Set<Long> distinct = new java.util.LinkedHashSet<>();
        for (Long id : entityIds) {
            if (id != null && id > 0L) {
                distinct.add(id);
            }
        }
        if (distinct.isEmpty()) {
            return out;
        }
        // MIN(created_time) per entity matches the single-row "ORDER BY version ASC LIMIT 1"
        // semantic — the earliest version's created_time is the canonical creation timestamp.
        StringBuilder sql = new StringBuilder(
                "SELECT entity_id, MIN(created_time) AS first_created FROM ");
        sql.append(ContextInternalTables.DATABASE).append('.').append(ContextInternalTables.VERSIONS)
                .append(" WHERE entity_id IN (");
        boolean first = true;
        for (Long id : distinct) {
            if (!first) {
                sql.append(',');
            }
            sql.append(id);
            first = false;
        }
        sql.append(") GROUP BY entity_id");
        try {
            JsonArray rows = runQuery(sql.toString());
            for (JsonElement row : rows) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data.size() < 2 || data.get(0).isJsonNull() || data.get(1).isJsonNull()) {
                    continue;
                }
                out.put(data.get(0).getAsLong(), data.get(1).getAsString());
            }
        } catch (Exception e) {
            // Heads/versions table may not be ready yet on a fresh FE; same fallback as the
            // single-row path — caller treats missing entries as "use now()".
            LOG.debug("loadCreatedTimes failed: {}", e.getMessage());
        }
        return out;
    }

    /**
     * Look up the {@code contextbase_id} that owns the given {@code entityId}. Used by the read
     * REST endpoints that take only an id but still need to gate the response on per-base
     * authorization. Returns {@code -1L} when the entity does not exist or the heads table is not
     * yet materialized.
     */
    public long resolveContextBaseIdForEntity(long entityId) {
        String sql = String.format(
                "SELECT contextbase_id FROM %s.%s WHERE entity_id = %d LIMIT 1",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, entityId);
        JsonArray rows = runQuery(sql);
        if (rows.size() == 0) {
            return -1L;
        }
        JsonArray data = rows.get(0).getAsJsonObject().getAsJsonArray("data");
        if (data.size() == 0 || data.get(0).isJsonNull()) {
            return -1L;
        }
        return data.get(0).getAsLong();
    }

    public VersionRow loadCurrentVersionRow(long entityId) {
        String sql = String.format(
                "SELECT v.entity_id, v.version, v.entity_key, v.entity_type, v.contextbase_id, v.collection_id, "
                        + "v.title, v.preview, v.body, v.raw_markdown, v.frontmatter_json, v.source_json, "
                        + "v.confidence, v.created_time, v.updated_time, v.commit_time, v.snapshot_version, v.deleted "
                        + "FROM %s.%s h JOIN %s.%s v "
                        + "ON h.entity_id = v.entity_id AND h.current_version = v.version "
                        + "WHERE h.entity_id = %d LIMIT 1",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                entityId);
        return decodeVersionRow(runQuery(sql));
    }

    public VersionRow loadVersionRow(long entityId, long version) {
        String sql = String.format(
                "SELECT entity_id, version, entity_key, entity_type, contextbase_id, collection_id, title, preview, body, "
                        + "raw_markdown, frontmatter_json, source_json, confidence, created_time, updated_time, commit_time, "
                        + "snapshot_version, deleted "
                        + "FROM %s.%s WHERE entity_id = %d AND version = %d LIMIT 1",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId, version);
        return decodeVersionRow(runQuery(sql));
    }

    /**
     * Bulk version-row loader. Used by retrieval flows (e.g. {@code ContextBudgetPlanner}) that
     * need bodies for many candidates at once — issues one PK-friendly OR-list SELECT instead of
     * N follow-up {@link #loadVersionRow} calls. Pairs that don't resolve (entity dropped, version
     * compacted) are simply absent from the returned map.
     */
    public Map<EntityVersionKey, VersionRow> loadVersionRows(Collection<EntityVersionKey> keys) {
        if (keys == null || keys.isEmpty()) {
            return new HashMap<>();
        }
        StringBuilder sb = new StringBuilder(192 + keys.size() * 32);
        sb.append("SELECT entity_id, version, entity_key, entity_type, contextbase_id, collection_id, "
                        + "title, preview, body, raw_markdown, frontmatter_json, source_json, confidence, "
                        + "created_time, updated_time, commit_time, snapshot_version, deleted FROM ")
                .append(ContextInternalTables.DATABASE).append('.').append(ContextInternalTables.VERSIONS)
                .append(" WHERE ");
        boolean first = true;
        for (EntityVersionKey k : keys) {
            if (!first) {
                sb.append(" OR ");
            }
            sb.append("(entity_id = ").append(k.entityId).append(" AND version = ").append(k.version).append(')');
            first = false;
        }
        JsonArray rows = runQuery(sb.toString());
        Map<EntityVersionKey, VersionRow> out = new HashMap<>(rows.size() * 2);
        for (JsonElement row : rows) {
            JsonArray asArray = new JsonArray();
            asArray.add(row);
            VersionRow decoded = decodeVersionRow(asArray);
            if (decoded != null) {
                out.put(new EntityVersionKey(decoded.entityId, decoded.version), decoded);
            }
        }
        return out;
    }

    /**
     * Highest persisted {@code version} for this {@code entity_id}. Returns 0 when the entity has
     * no versions yet (e.g., brand-new entity) or when the lookup can't run (e.g., internal
     * tables not yet provisioned). The {@link com.starrocks.context.allocator.ContextVersionAllocator}
     * uses this to lazily seed its in-memory counter on first call per entity, so the storage
     * table — not the in-memory map — is the source of truth for monotonic version assignment.
     */
    public long maxVersionOf(long entityId) {
        String sql = String.format(
                "SELECT MAX(version) FROM %s.%s WHERE entity_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId);
        try {
            long observed = runScalarLong(sql);
            return Math.max(0L, observed);
        } catch (Exception e) {
            return 0L;
        }
    }

    public long resolveVersionAsOf(long entityId, String asOfTime) {
        String normalized = asOfTime == null ? null : asOfTime.trim();
        if (Strings.isNullOrEmpty(normalized)) {
            return -1L;
        }
        // Match SnapshotResolver.resolveFromSelector semantics: accept either a numeric
        // snapshot_version fence or an ISO timestamp. Without this dual path, callers passing
        // "19019" (a snapshot_version that read-contextbase / read-collection accept) hit
        // updated_time <= '19019' here and the lexicographic compare against actual timestamps
        // like '2026-04-29 09:01:28' silently returns no rows — looks like the entity didn't
        // exist at that point in time when in fact context/get just couldn't parse the selector.
        try {
            long snapshotFence = Long.parseLong(normalized);
            String sql = String.format(
                    "SELECT MAX(version) FROM %s.%s WHERE entity_id = %d AND snapshot_version <= %d",
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                    entityId, snapshotFence);
            return runScalarLong(sql);
        } catch (NumberFormatException ignored) {
            // not a snapshot_version — fall through to timestamp path
        }
        if (normalized.length() == 10) {
            normalized = normalized + " 23:59:59";
        }
        String sql = String.format(
                "SELECT MAX(version) FROM %s.%s WHERE entity_id = %d AND updated_time <= '%s'",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                entityId, escapeSql(normalized));
        return runScalarLong(sql);
    }

    /**
     * Bulk neighbour-preview loader keyed by seed {@code (entity_id, version)}. Used by
     * {@code ContextBudgetPlanner} so that planning N candidates issues one neighbour query
     * instead of N. Per-seed cap on the returned neighbours is enforced server-side via
     * {@code ROW_NUMBER() OVER (PARTITION BY ...)} so a seed with thousands of outgoing edges
     * cannot blow up the result set.
     */
    public Map<EntityVersionKey, JsonArray> getNeighbourPreviewsBulk(Collection<EntityVersionKey> seeds,
                                                                     long snapshotFence,
                                                                     int maxNeighboursPerSeed) {
        Map<EntityVersionKey, JsonArray> out = new HashMap<>();
        if (seeds == null || seeds.isEmpty()) {
            return out;
        }
        int limit = maxNeighboursPerSeed > 0 ? maxNeighboursPerSeed : 32;
        StringBuilder seedClause = new StringBuilder(seeds.size() * 32);
        boolean first = true;
        for (EntityVersionKey s : seeds) {
            if (!first) {
                seedClause.append(" OR ");
            }
            seedClause.append("(r.src_entity_id = ").append(s.entityId)
                    .append(" AND r.src_version = ").append(s.version).append(')');
            first = false;
            // Pre-seed the map so callers can read the empty array even for seeds with no edges.
            out.put(s, new JsonArray());
        }
        StringBuilder dstSeedClause = new StringBuilder(seeds.size() * 32);
        first = true;
        for (EntityVersionKey s : seeds) {
            if (!first) {
                dstSeedClause.append(" OR ");
            }
            dstSeedClause.append("(r2.src_entity_id = ").append(s.entityId)
                    .append(" AND r2.src_version = ").append(s.version).append(')');
            first = false;
        }
        String asOfFilter = snapshotFence >= 0 ? " AND vv.snapshot_version <= " + snapshotFence : "";
        // Inner subquery: per dst entity, find the max version visible at the snapshot fence,
        // filtered to entities reachable from any seed (so we don't aggregate the entire
        // versions table for unrelated rows).
        // Outer wrapper: ROW_NUMBER() per seed so each seed gets at most `limit` neighbours.
        String sql = String.format(
                "SELECT src_entity_id, src_version, entity_id, entity_key, preview, snapshot_version FROM ("
                        + "SELECT r.src_entity_id, r.src_version, v.entity_id, v.entity_key, v.preview, "
                        + "v.snapshot_version, ROW_NUMBER() OVER ("
                        + "PARTITION BY r.src_entity_id, r.src_version ORDER BY v.entity_id) AS rn "
                        + "FROM %s.%s r "
                        + "JOIN ("
                        + "  SELECT vv.entity_id AS dst_entity_id, MAX(vv.version) AS max_version FROM %s.%s vv "
                        + "  WHERE vv.entity_id IN ("
                        + "    SELECT DISTINCT r2.dst_entity_id FROM %s.%s r2 WHERE %s"
                        + "  )%s GROUP BY vv.entity_id"
                        + ") t ON t.dst_entity_id = r.dst_entity_id "
                        + "JOIN %s.%s v ON v.entity_id = t.dst_entity_id AND v.version = t.max_version "
                        + "WHERE %s"
                        + ") ranked WHERE rn <= %d ORDER BY src_entity_id, src_version, entity_id",
                ContextInternalTables.DATABASE, ContextInternalTables.REFS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                ContextInternalTables.DATABASE, ContextInternalTables.REFS, dstSeedClause,
                asOfFilter,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                seedClause, limit);
        JsonArray rows = runQuery(sql);
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            long srcEntity = data.get(0).getAsLong();
            long srcVersion = data.get(1).getAsLong();
            EntityVersionKey key = new EntityVersionKey(srcEntity, srcVersion);
            // Re-shape the row into the four-column tuple the original
            // {@link #getNeighbourPreviews} returned: (entity_id, entity_key, preview,
            // snapshot_version) so callers can stay on the existing decode path.
            JsonArray reshapedData = new JsonArray();
            reshapedData.add(data.get(2));
            reshapedData.add(data.get(3));
            reshapedData.add(data.get(4));
            reshapedData.add(data.get(5));
            com.google.gson.JsonObject reshapedRow = new com.google.gson.JsonObject();
            reshapedRow.add("data", reshapedData);
            out.computeIfAbsent(key, k -> new JsonArray()).add(reshapedRow);
        }
        return out;
    }

    public JsonArray getNeighbourPreviews(long seedEntityId, long seedVersion, long snapshotFence, int maxNeighbours) {
        return runQuery(buildNeighbourPreviewsSql(seedEntityId, seedVersion, snapshotFence, maxNeighbours));
    }

    public JsonArray getNeighbourBodies(long seedEntityId, long seedVersion, long snapshotFence, int maxNeighbours) {
        return runQuery(buildNeighbourBodiesSql(seedEntityId, seedVersion, snapshotFence, maxNeighbours));
    }

    /**
     * Sub-SELECT yielding the destination entity ids of the seed's <em>active</em> references.
     * REFERENCE_RESYNC appends a fresh row per {@code (src_entity_id, src_version, ord)} at a new
     * {@code snapshot_version} rather than deleting the old one (see the {@code context_entity_refs}
     * PK in {@link ContextInternalTables}), so a read must resolve, per ordinal, the row with the
     * greatest {@code snapshot_version} that is within the fence — {@code MAX(snapshot_version)} for
     * a current read ({@code snapshotFence < 0}), or {@code MAX(snapshot_version) <= fence} for an
     * as-of read. Collecting every ref row (as a plain {@code DISTINCT} would) leaks references
     * created after the fence and follows superseded destinations. Package-private for shape testing.
     */
    static String buildActiveRefDstSubquery(long seedEntityId, long seedVersion, long snapshotFence) {
        String refFence = snapshotFence >= 0 ? " AND snapshot_version <= " + snapshotFence : "";
        return String.format(
                "SELECT r.dst_entity_id FROM %s.%s r "
                        + "JOIN (SELECT ord, MAX(snapshot_version) AS active_sv FROM %s.%s "
                        + "      WHERE src_entity_id = %d AND src_version = %d%s GROUP BY ord) am "
                        + "  ON r.ord = am.ord AND r.snapshot_version = am.active_sv "
                        + "WHERE r.src_entity_id = %d AND r.src_version = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.REFS,
                ContextInternalTables.DATABASE, ContextInternalTables.REFS,
                seedEntityId, seedVersion, refFence, seedEntityId, seedVersion);
    }

    static String buildNeighbourPreviewsSql(long seedEntityId, long seedVersion, long snapshotFence, int maxNeighbours) {
        int limit = maxNeighbours > 0 ? maxNeighbours : 32;
        String asOfFilter = snapshotFence >= 0 ? " AND vv.snapshot_version <= " + snapshotFence : "";
        return String.format(
                "SELECT v.entity_id, v.entity_key, v.preview, v.snapshot_version "
                        + "FROM %s.%s v "
                        + "JOIN ("
                        + "  SELECT vv.entity_id AS dst_entity_id, MAX(vv.version) AS max_version FROM %s.%s vv "
                        + "  WHERE vv.entity_id IN (%s)"
                        + "  %s GROUP BY vv.entity_id"
                        + ") t ON v.entity_id = t.dst_entity_id AND v.version = t.max_version "
                        + "ORDER BY v.entity_id LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                buildActiveRefDstSubquery(seedEntityId, seedVersion, snapshotFence),
                asOfFilter, limit);
    }

    static String buildNeighbourBodiesSql(long seedEntityId, long seedVersion, long snapshotFence, int maxNeighbours) {
        int limit = maxNeighbours > 0 ? maxNeighbours : 8;
        String asOfFilter = snapshotFence >= 0 ? " AND vv.snapshot_version <= " + snapshotFence : "";
        return String.format(
                "SELECT v.entity_id, v.version, v.entity_key, v.title, v.body, v.snapshot_version "
                        + "FROM %s.%s v "
                        + "JOIN ("
                        + "  SELECT vv.entity_id AS dst, MAX(vv.version) AS dst_v "
                        + "  FROM %s.%s vv "
                        + "  WHERE vv.entity_id IN (%s)"
                        + "  %s GROUP BY vv.entity_id"
                        + ") t ON v.entity_id = t.dst AND v.version = t.dst_v "
                        + "WHERE v.deleted = false ORDER BY v.entity_id LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                buildActiveRefDstSubquery(seedEntityId, seedVersion, snapshotFence),
                asOfFilter, limit);
    }

    private VersionRow decodeVersionRow(JsonArray rows) {
        if (rows == null || rows.size() == 0) {
            return null;
        }
        JsonArray data = rows.get(0).getAsJsonObject().getAsJsonArray("data");
        return new VersionRow(
                data.size() > 0 && !data.get(0).isJsonNull() ? data.get(0).getAsLong() : 0L,
                data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsLong() : 0L,
                data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsString() : null,
                data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsString() : null,
                data.size() > 4 && !data.get(4).isJsonNull() ? data.get(4).getAsLong() : 0L,
                data.size() > 5 && !data.get(5).isJsonNull() ? data.get(5).getAsLong() : 0L,
                data.size() > 6 && !data.get(6).isJsonNull() ? data.get(6).getAsString() : null,
                data.size() > 7 && !data.get(7).isJsonNull() ? data.get(7).getAsString() : null,
                data.size() > 8 && !data.get(8).isJsonNull() ? data.get(8).getAsString() : null,
                data.size() > 9 && !data.get(9).isJsonNull() ? data.get(9).getAsString() : null,
                // frontmatter_json and source_json are JSON-typed columns. The result protocol
                // sends them as JsonObject / JsonArray, not primitive strings — calling
                // .getAsString() on those throws UnsupportedOperationException("JsonObject").
                // Use .toString() so primitives stay primitive and structured values become
                // their serialized JSON form, then unwrap the outer quotes for primitives below.
                data.size() > 10 && !data.get(10).isJsonNull()
                        ? jsonElementToRawString(data.get(10)) : null,
                data.size() > 11 && !data.get(11).isJsonNull()
                        ? jsonElementToRawString(data.get(11)) : null,
                data.size() > 12 && !data.get(12).isJsonNull() ? data.get(12).getAsDouble() : 0.0,
                data.size() > 13 && !data.get(13).isJsonNull() ? data.get(13).getAsString() : null,
                data.size() > 14 && !data.get(14).isJsonNull() ? data.get(14).getAsString() : null,
                data.size() > 15 && !data.get(15).isJsonNull() ? data.get(15).getAsString() : null,
                data.size() > 16 && !data.get(16).isJsonNull() ? data.get(16).getAsLong() : 0L,
                data.size() > 17 && ContextJsonUtil.parseBool(data.get(17)));
    }

    private static String jsonElementToRawString(com.google.gson.JsonElement el) {
        if (el.isJsonPrimitive()) {
            return el.getAsString();
        }
        // Objects and arrays serialize back to their JSON text form.
        return el.toString();
    }

    private JsonArray runQuery(String sql) {
        return ContextSqlSupport.executeDql(sql);
    }

    private static String escapeSql(String s) {
        return ContextSqlEscape.body(s);
    }

    /**
     * Map key for the bulk version-row and bulk neighbour-preview loaders. Pairs
     * {@code (entity_id, version)} as a single hashable key — reused by retrieval flows that need
     * to fan-out one query across many candidates and demux the results back into per-candidate
     * caches.
     */
    public static final class EntityVersionKey {
        public final long entityId;
        public final long version;

        public EntityVersionKey(long entityId, long version) {
            this.entityId = entityId;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EntityVersionKey)) {
                return false;
            }
            EntityVersionKey other = (EntityVersionKey) o;
            return entityId == other.entityId && version == other.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityId, version);
        }

        @Override
        public String toString() {
            return "(entity_id=" + entityId + ", version=" + version + ")";
        }
    }
}
