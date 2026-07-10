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

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.gson.stream.JsonWriter;
import com.starrocks.common.AuditLog;
import com.starrocks.context.allocator.ContextIdAllocator;
import com.starrocks.context.allocator.ContextSnapshotAllocator;
import com.starrocks.context.allocator.ContextVersionAllocator;
import com.starrocks.context.embedding.FeEmbeddingClient;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.context.markdown.MarkdownExtractor;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.summary.StreamLoader;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Writes a semantic-context entity upsert to the three tables that gate external visibility:
 * {@link ContextInternalTables#VERSIONS versions}, {@link ContextInternalTables#HEADS heads}, and
 * {@link ContextInternalTables#COMMITS commits}. Fragment extraction and reference-edge writes are
 * Milestone 3 work, so this executor deliberately does not touch
 * {@link ContextInternalTables#FRAGMENTS} or {@link ContextInternalTables#REFS}.
 *
 * <p>Concurrency model: each call acquires a new {@code (id, version, snapshot_version)} triple via
 * the allocators and emits three sequential INSERT statements through {@link SimpleExecutor}. The
 * Primary Key engine handles the heads-row replace semantics; no separate UPSERT statement is needed.
 */
public class ContextWriteExecutor {

    private static final Logger LOG = LogManager.getLogger(ContextWriteExecutor.class);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int PREVIEW_MAX_CHARS = 512;
    // A digit-only entity_key would make `[[e:12552]]` ambiguous: it could mean the entity with
    // id=12552 OR the entity whose entity_key is the string "12552". The extractor picks the
    // numeric branch first (id), so a digit-only key could never be referenced from markdown
    // anyway. We reject at write time to fail loudly rather than silently.
    private static final Pattern DIGIT_ONLY_ENTITY_KEY = Pattern.compile("^\\d+$");

    private final ContextMgr contextMgr;
    private final ContextVersionAllocator versionAllocator;
    private final ContextSnapshotAllocator snapshotAllocator;

    public ContextWriteExecutor(ContextMgr contextMgr,
                                ContextVersionAllocator versionAllocator,
                                ContextSnapshotAllocator snapshotAllocator) {
        this.contextMgr = contextMgr;
        this.versionAllocator = versionAllocator;
        this.snapshotAllocator = snapshotAllocator;
    }

    /**
     * Result of a single upsert: the canonical identifier triple plus the resolved entity_key.
     */
    public static final class UpsertResult {
        public final long entityId;
        public final long version;
        public final long snapshotVersion;
        public final String entityKey;

        UpsertResult(long entityId, long version, long snapshotVersion, String entityKey) {
            this.entityId = entityId;
            this.version = version;
            this.snapshotVersion = snapshotVersion;
            this.entityKey = entityKey;
        }
    }

    /**
     * Per-row outcome from {@link #upsertBatch}. Carries either a successful {@link UpsertResult}
     * or a failure message; never both. The list returned by upsertBatch is parallel to its input
     * list so callers can mark per-row error isolation in their REST/daemon response shape
     * without re-correlating.
     */
    public static final class UpsertOutcome {
        public final int index;
        public final boolean ok;
        public final UpsertResult result;
        public final String errorMessage;

        private UpsertOutcome(int index, boolean ok, UpsertResult result, String errorMessage) {
            this.index = index;
            this.ok = ok;
            this.result = result;
            this.errorMessage = errorMessage;
        }

        static UpsertOutcome success(int index, UpsertResult result) {
            return new UpsertOutcome(index, true, result, null);
        }

        static UpsertOutcome failure(int index, String message) {
            return new UpsertOutcome(index, false, null, message);
        }
    }

    public UpsertResult upsert(ContextCollectionName collection,
                               Map<String, Expr> entityArgs,
                               Map<String, Expr> options) {
        return upsert(collection, entityArgs, /*edges*/ null, options);
    }

    /**
     * Same as {@link #upsert(ContextCollectionName, Map, Map)} but threads through an explicit
     * {@code EDGES (...)} list. Each edge expression is interpreted as the destination entity_id
     * for a {@code ref_kind='explicit'} row, written alongside the inline `[[e:id]]` refs that the
     * markdown body produces. The split exists because the SQL grammar exposes the EDGES clause
     * separately from ENTITY (...) — the executor needs both to produce a complete refs row set.
     */
    public UpsertResult upsert(ContextCollectionName collection,
                               Map<String, Expr> entityArgs,
                               java.util.List<Expr> edges,
                               Map<String, Expr> options) {
        return upsert(collection, entityArgs, edges, options, false);
    }

    /**
     * Internal upsert that threads a {@code tombstone} flag through to the versions / heads
     * INSERTs. When {@code tombstone=true}, this write represents a soft-delete event and lands
     * with {@code deleted=true} on the new version and {@code current_deleted=true} on the head
     * row. Retrieval paths ({@code vector_search}, {@code text_search}, {@code read_collection},
     * {@code reference_expand}, channel pulls) all filter on {@code deleted=false} /
     * {@code current_deleted=false}, so this is the signal that actually removes the entity from
     * search results — confidence=0.0 alone wasn't enough because no reader filters on confidence.
     */
    private UpsertResult upsert(ContextCollectionName collection,
                                Map<String, Expr> entityArgs,
                                java.util.List<Expr> edges,
                                Map<String, Expr> options,
                                boolean tombstone) {
        if (collection.getContextBase() == null) {
            throw new IllegalArgumentException("collection must be qualified as contextbase.collection");
        }
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(collection.getContextBase());
        if (cb == null) {
            throw new IllegalStateException("contextbase not found: " + collection.getContextBase());
        }
        ContextMgr.CollectionMeta col = findCollection(collection);
        if (col == null) {
            throw new IllegalStateException("collection not found: " + collection);
        }
        // Fail-loud BEFORE allocating snapshot_version / entity_id / running any DML. Without the
        // pre-check a misconfigured cluster used to leave versions/heads/commits behind and silently
        // skip the fragments INSERT — entities existed but were invisible to text + vector search.
        com.starrocks.context.embedding.EmbeddingConfigJson.requireBuild();

        String entityKey = validateEntityKey(stringArg(entityArgs, "entity_key"));
        // entity_type must be supplied explicitly per the API design §7.2 — silently defaulting
        // to the collection type bypasses the {collection_type → entity_type} matrix and lets a
        // caller insert e.g. an `object` entity into a `channel` collection. Validate against
        // the matrix and let the caller see the actionable error from CollectionTypePolicy.
        String entityType = stringArg(entityArgs, "entity_type");
        com.starrocks.context.policy.CollectionTypePolicy.check(col.getCollectionType(), entityType);
        String title = stringArg(entityArgs, "title");
        String providedPreview = stringArg(entityArgs, "preview");
        String rawBody = orDefault(stringArg(entityArgs, "content"), "");
        double confidence = doubleArg(entityArgs, "confidence", 1.0);
        String requestId = stringArg(options, "request_id");

        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(rawBody, providedPreview);
        String body = extracted.body;
        String preview = extracted.preview;
        if (Strings.isNullOrEmpty(preview)) {
            preview = truncate(body, PREVIEW_MAX_CHARS);
        }

        // Versioned entity identity: when a prior upsert with the same entity_key exists *in this
        // collection*, we reuse the existing entity_id and bump its version. Allocating a fresh
        // id every time would create a new "logical" entity per upsert, which breaks the
        // documented contract that CONTEXT UPSERT produces a new *version* of the named entity.
        //
        // The lookup must be scoped to (contextbase_id, collection_id). Without scope, the same
        // entity_key in a different collection / base would route this upsert to the wrong logical
        // entity — appending a version to someone else's history. Scope is the architecture doc
        // §5 invariant ("entity_key is unique per collection, not globally").
        long explicitId = longArg(entityArgs, "id", -1L);
        if (explicitId < 0L) {
            explicitId = longArg(entityArgs, "entity_id", -1L);
        }
        long entityId = explicitId;
        if (entityId < 0L && !Strings.isNullOrEmpty(entityKey)) {
            // Must distinguish "key genuinely not present" (returns 0 → allocate fresh id) from
            // "lookup failed for an infrastructure reason" (storage flake, SQL transient). The
            // earlier catch-all swallowed both and silently allocated a new entity_id on transient
            // SQL hiccup, forking the entity's history: subsequent successful upserts with the
            // same entity_key would either fork again or land on a different fresh id, orphaning
            // every prior version under an unreferenced id. Architecture §5 requires entity_key
            // to map deterministically to one entity_id per (contextbase, collection); silently
            // re-allocating violates that invariant. Propagate the exception so the row fails
            // fast and the caller can retry against a consistent storage state.
            long existing = com.starrocks.server.GlobalStateMgr.getCurrentState()
                    .getContextReadExecutor()
                    .resolveEntityIdByKey(entityKey, cb.getId(), col.getId());
            if (existing > 0L) {
                entityId = existing;
            }
        }
        if (entityId < 0L) {
            entityId = ContextIdAllocator.next();
        } else if (explicitId >= 0L) {
            // An explicit id/entity_id bypasses the scoped entity_key lookup above. Verify it
            // actually lives in this (contextbase, collection) before bumping its version:
            // context_entity_heads is keyed by entity_id alone, so an id belonging to another
            // base would otherwise append a version under this base AND replace that base's
            // current head. Mismatch throws (opaque "entity not found", same as updateMetadata).
            currentRowInScope(entityId, cb.getId(), col.getId());
        }
        long version = versionAllocator.next(entityId);
        long snapshotVersion = snapshotAllocator.next();
        String now = TS_FMT.format(LocalDateTime.now());
        String createdTime = lookupCurrentCreatedTime(entityId);
        if (Strings.isNullOrEmpty(createdTime)) {
            createdTime = now;
        }
        SimpleExecutor executor = SimpleExecutor.getRepoExecutor();

        String versionsInsert = String.format(
                "INSERT INTO %s.%s ("
                        + "entity_id, version, entity_key, contextbase_id, collection_id, collection_type, entity_type, "
                        + "status, title, preview, body, raw_markdown, frontmatter_json, source_json, "
                        + "confidence, body_token_count, "
                        + "created_time, updated_time, commit_time, snapshot_version, request_id, deleted) "
                        + "VALUES (%d, %d, %s, %d, %d, %s, %s, 'ACTIVE', %s, %s, %s, %s, %s, %s, %f, %d, "
                        + "'%s', '%s', '%s', %d, %s, %b)",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                entityId, version, sqlStringOrNull(entityKey),
                cb.getId(), col.getId(),
                sqlString(col.getCollectionType()), sqlString(entityType),
                sqlStringOrNull(title), sqlString(preview), sqlString(body),
                sqlString(rawBody),
                sqlStringOrNull(extracted.frontmatterJson),
                sqlStringOrNull(extracted.sourceJson),
                confidence, (long) body.length(),
                createdTime, now, now, snapshotVersion,
                sqlStringOrNull(requestId),
                tombstone);

        // Recovery: any of the 5 INSERTs below can fail (BDBJE flake, disk pressure, retry
        // mid-flight). Heads is written LAST so readers don't see partial state — they keep
        // observing the previous head until heads.current_version advances. But orphan rows in
        // versions / fragments / refs / commits would accumulate forever without cleanup.
        // Wrap the publish chain in a try/catch that issues best-effort compensating DELETEs
        // when any step before heads fails. If the DELETEs themselves fail we re-throw the
        // original cause; a subsequent identical upsert allocates a fresh version/snapshot so
        // the orphans stay invisible but unreclaimed (acceptable degradation vs. losing the
        // original error).
        try {
            executor.executeDML(versionsInsert);

            // Order matters for partial-failure visibility. Heads is the user-facing pointer
            // ({@code current_version, current_snapshot_version}); once it advances, every read of
            // the entity sees the new version and goes looking for matching fragments / refs /
            // commit row. The previous order was heads → fragments → refs → commits, which meant
            // any failure between the heads INSERT and the commits INSERT exposed a head that
            // pointed at incomplete state — text/vector search would return zero rows, snapshot
            // fences would fall off the commits row, and the caller's error did not undo any of
            // it. We now write data first, commit metadata next, and flip heads LAST so a failure
            // anywhere upstream leaves the entity at its previous version (or absent on first
            // insert) and the orphan rows are invisible to readers.
            writeFragments(executor, entityId, version, snapshotVersion, cb.getId(), col.getId(), preview, extracted);
            writeRefs(executor, entityId, version, snapshotVersion, cb.getId(), cb.getName(),
                    extracted, edges);

            String commitsInsert = String.format(
                    "INSERT INTO %s.%s ("
                            + "snapshot_version, contextbase_id, request_id, commit_time, "
                            + "visibility_state, primary_ready, refs_ready, fragments_ready, error_message) "
                            + "VALUES (%d, %d, %s, '%s', 'VISIBLE', true, true, true, NULL)",
                    ContextInternalTables.DATABASE, ContextInternalTables.COMMITS,
                    snapshotVersion, cb.getId(), sqlStringOrNull(requestId), now);
            executor.executeDML(commitsInsert);

            String headsInsert = String.format(
                    "INSERT INTO %s.%s ("
                            + "entity_id, entity_key, contextbase_id, collection_id, collection_type, entity_type, "
                            + "current_version, current_snapshot_version, current_preview, current_confidence, "
                            + "current_updated_time, current_deleted, "
                            + "last_ref_version, last_fragment_version) "
                            + "VALUES (%d, %s, %d, %d, %s, %s, %d, %d, %s, %f, '%s', %b, %d, %d)",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                    entityId, sqlStringOrNull(entityKey),
                    cb.getId(), col.getId(),
                    sqlString(col.getCollectionType()), sqlString(entityType),
                    version, snapshotVersion, sqlString(preview), confidence, now,
                    tombstone, version, version);
            executor.executeDML(headsInsert);
        } catch (RuntimeException publishFailure) {
            cleanupOrphansBestEffort(executor, entityId, version, snapshotVersion);
            throw publishFailure;
        }

        LOG.info("context upsert ok: id={} version={} snapshot={} collection={}",
                entityId, version, snapshotVersion, collection);
        MetricRepo.COUNTER_CONTEXT_UPSERT_TOTAL.increase(1L);
        AuditLog.getInternalAudit().info(
                "context_upsert | collection={} | entity_id={} version={} snapshot_version={} "
                        + "entity_key={} request_id={}",
                collection, entityId, version, snapshotVersion,
                entityKey == null ? "" : entityKey,
                requestId == null ? "" : requestId);
        return new UpsertResult(entityId, version, snapshotVersion, entityKey);
    }

    /**
     * Soft-delete: preserve the current entity content/provenance and write a new current version
     * with {@code confidence=0.0}. Hard delete remains the only path that physically removes the
     * entity from storage.
     */
    public UpsertResult tombstone(ContextCollectionName collection, long entityId, String entityKey,
                                  Map<String, Expr> options) {
        ContextReadExecutor.VersionRow current = com.starrocks.server.GlobalStateMgr.getCurrentState()
                .getContextReadExecutor().loadCurrentVersionRow(entityId);
        if (current == null) {
            throw new IllegalStateException("entity not found: " + entityId);
        }
        // We already paid for the read; warm the version allocator with what we just observed so
        // the upsert below allocates current.version + 1 instead of going through a lazy fallback.
        versionAllocator.seed(entityId, current.version);
        Map<String, Expr> entityArgs = new java.util.LinkedHashMap<>();
        entityArgs.put("id", new IntLiteral(current.entityId));
        if (!Strings.isNullOrEmpty(current.entityKey)) {
            entityArgs.put("entity_key", new StringLiteral(current.entityKey));
        }
        entityArgs.put("entity_type", new StringLiteral(current.entityType));
        if (current.title != null) {
            entityArgs.put("title", new StringLiteral(current.title));
        }
        if (!Strings.isNullOrEmpty(current.preview)) {
            entityArgs.put("preview", new StringLiteral(current.preview));
        }
        entityArgs.put("confidence", new FloatLiteral(0.0));
        entityArgs.put("content", new StringLiteral(current.effectiveRawMarkdown()));
        // Route through the tombstone-aware upsert so the new version lands with deleted=true and
        // the head row gets current_deleted=true. Retrieval paths filter on those columns; without
        // the flag a soft-deleted entity would still surface in vector_search / text_search /
        // read_collection / reference_expand / channel pulls.
        UpsertResult result = upsert(collection, entityArgs, /*edges*/ null, options, /*tombstone*/ true);
        MetricRepo.COUNTER_CONTEXT_DELETE_TOTAL.increase(1L);
        AuditLog.getInternalAudit().info(
                "context_delete | collection={} | entity_id={} tombstone_version={} snapshot_version={} entity_key={}",
                collection, result.entityId, result.version, result.snapshotVersion,
                result.entityKey == null ? "" : result.entityKey);
        return result;
    }

    /**
     * Metadata-only update: replace the current version's {@code frontmatter_json} in place
     * WITHOUT re-embedding the body. The entity's body, preview, fragments (and their vectors),
     * refs, version and snapshot are all left untouched — only {@code frontmatter_json} and
     * {@code updated_time} on the current version row change, plus a best-effort
     * {@code current_updated_time} bump on the head.
     *
     * <p>This is the cheap path for high-frequency metadata writes (e.g. verify-on-use staleness
     * state) that live in frontmatter but do not change the embedded text. Contract: the caller
     * supplies the COMPLETE frontmatter JSON; it replaces the stored value wholesale (no per-key
     * merge). Unlike {@link #upsert} this issues ZERO embedding calls and does not allocate a new
     * version — it relaxes per-version immutability for the current row only, which is acceptable
     * because frontmatter is mutable current-state, not content history. Older version rows are
     * never touched, so exact-version / history reads of prior versions are unaffected.
     */
    public UpsertResult updateMetadata(ContextCollectionName collection, Long entityId, String entityKey,
                                       String frontmatterJson) {
        if (collection.getContextBase() == null) {
            throw new IllegalArgumentException("collection must be qualified as contextbase.collection");
        }
        if (Strings.isNullOrEmpty(frontmatterJson)) {
            throw new IllegalArgumentException("frontmatter is required");
        }
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(collection.getContextBase());
        if (cb == null) {
            throw new IllegalStateException("contextbase not found: " + collection.getContextBase());
        }
        ContextMgr.CollectionMeta col = findCollection(collection);
        if (col == null) {
            throw new IllegalStateException("collection not found: " + collection);
        }

        // Resolve the logical entity_id. entity_key lookup is scoped to (contextbase, collection)
        // — the same §5 invariant the upsert path enforces.
        long id = entityId == null ? -1L : entityId;
        if (id < 0L) {
            String key = validateEntityKey(entityKey);
            if (Strings.isNullOrEmpty(key)) {
                throw new IllegalArgumentException("one of \"id\"/\"entity_key\" is required");
            }
            long existing = com.starrocks.server.GlobalStateMgr.getCurrentState()
                    .getContextReadExecutor().resolveEntityIdByKey(key, cb.getId(), col.getId());
            if (existing <= 0L) {
                throw new IllegalStateException("entity not found for key: " + key);
            }
            id = existing;
        }

        ContextReadExecutor.VersionRow current = com.starrocks.server.GlobalStateMgr.getCurrentState()
                .getContextReadExecutor().loadCurrentVersionRow(id);
        if (current == null) {
            throw new IllegalStateException("entity not found: " + id);
        }
        // entity_id is a GLOBAL sequence shared across every contextbase; the REST auth check only
        // validated USAGE on the NAMED contextbase. Confirm the loaded row actually lives in that
        // contextbase/collection before touching it — otherwise a caller authorized on contextbase A
        // could overwrite an entity in contextbase B by passing B's id. This mirrors the canonical
        // write path's guard in ContextCommandService.resolveRow. The entity_key path is already
        // scoped via resolveEntityIdByKey(key, cb.getId(), col.getId()) above; this closes the id path.
        // Reuse the same "entity not found" message so the response cannot be used to probe whether an
        // id exists in some other contextbase.
        if (current.contextBaseId != cb.getId() || current.collectionId != col.getId()) {
            throw new IllegalStateException("entity not found: " + id);
        }

        String now = TS_FMT.format(LocalDateTime.now());
        SimpleExecutor executor = SimpleExecutor.getRepoExecutor();

        // In-place UPDATE of the current version row. frontmatter_json is a JSON column; a string
        // literal is implicitly cast on assignment exactly as on the INSERT path (see versionsInsert
        // above). version is bound to the value just read rather than a subquery — a concurrent
        // content upsert advancing the head between read and update is an accepted, rare race for a
        // metadata-only write.
        executor.executeDML(String.format(
                "UPDATE %s.%s SET frontmatter_json = %s, updated_time = '%s' "
                        + "WHERE entity_id = %d AND version = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                sqlStringOrNull(frontmatterJson), now, id, current.version));

        // Keep the head's observability timestamp roughly in sync. Best-effort: frontmatter is not
        // cached on heads, so this is not required for read correctness.
        try {
            executor.executeDML(String.format(
                    "UPDATE %s.%s SET current_updated_time = '%s' WHERE entity_id = %d",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS, now, id));
        } catch (Exception e) {
            LOG.debug("heads current_updated_time bump failed for entity {}: {}", id, e.getMessage());
        }

        LOG.info("context update_metadata ok: id={} version={} collection={}", id, current.version, collection);
        AuditLog.getInternalAudit().info(
                "context_update_metadata | collection={} | entity_id={} version={} entity_key={}",
                collection, id, current.version, current.entityKey == null ? "" : current.entityKey);
        return new UpsertResult(id, current.version, current.snapshotVersion,
                current.entityKey != null ? current.entityKey : entityKey);
    }

    /**
     * Batch upsert. The single-row {@link #upsert} path issues ~5 sequential INSERTs; for bulk
     * import / workspace-commit / derived-page workloads that's N×5 round-trips serially on the
     * leader. This method collapses the batch into 5 multi-row INSERT statements (versions,
     * heads, fragments, refs, commits) regardless of batch size. Per-row error isolation is
     * preserved by failing rows during the Java-side validation phase before any SQL runs.
     *
     * <p>Phases:
     * <ol>
     *     <li>Validate + extract markdown per row. Bad rows record an error and are skipped.</li>
     *     <li>Bulk resolve {@code entity_key}s and explicit edge keys via two SELECTs.</li>
     *     <li>Allocate {@code (entity_id, version, snapshot_version)} per row in-memory.</li>
     *     <li>Bulk fetch {@code created_time} for any reused entity_ids.</li>
     *     <li>Emit ≤5 multi-row INSERTs. If any fails, all surviving rows are marked failed.</li>
     *     <li>Audit + metrics.</li>
     * </ol>
     *
     * @param collection         contextbase.collection target — same scope rules as {@link #upsert}
     * @param entityArgsList     per-row entity arguments (entity_key/entity_type/title/preview/content/...)
     * @param perEntityEdges     parallel-indexed edges per row, may be {@code null} for "no edges anywhere"
     * @param sharedOptions      request_id applied to every row
     * @return per-row outcomes in input order
     */
    public List<UpsertOutcome> upsertBatch(ContextCollectionName collection,
                                           List<Map<String, Expr>> entityArgsList,
                                           List<List<Expr>> perEntityEdges,
                                           Map<String, Expr> sharedOptions) {
        if (collection == null || collection.getContextBase() == null) {
            throw new IllegalArgumentException("collection must be qualified as contextbase.collection");
        }
        if (entityArgsList == null || entityArgsList.isEmpty()) {
            return Collections.emptyList();
        }
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(collection.getContextBase());
        if (cb == null) {
            throw new IllegalStateException("contextbase not found: " + collection.getContextBase());
        }
        ContextMgr.CollectionMeta col = findCollection(collection);
        if (col == null) {
            throw new IllegalStateException("collection not found: " + collection);
        }

        String requestId = stringArg(sharedOptions, "request_id");
        // Fail-loud BEFORE pre-validation / id allocation. The FE-side batch path computes
        // embeddings via FeEmbeddingClient in Phase 4.7, so we only need to confirm the provider
        // is configured here — the JSON itself is no longer threaded through to the INSERT
        // (fragments rows now carry array literals instead of `embedding(text, parse_json(cfg))`).
        com.starrocks.context.embedding.EmbeddingConfigJson.requireBuild();
        String now = TS_FMT.format(LocalDateTime.now());

        int n = entityArgsList.size();
        UpsertOutcome[] outcomes = new UpsertOutcome[n];
        Prepared[] prepared = new Prepared[n];
        Set<String> keysToResolve = new LinkedHashSet<>();
        Set<String> distinctEdgeKeys = new LinkedHashSet<>();
        Set<String> seenKeysInBatch = new HashSet<>();

        // PERF: temporary instrumentation — phase timers + per-DML byte/row counters fed back from
        // executeBatchedInserts. Diagnostic only; revert once the bulk-import bottleneck is fixed.
        Stopwatch swBatch = Stopwatch.createStarted();
        Stopwatch swP = Stopwatch.createStarted();
        long[] dmlMs = new long[5];
        long[] dmlBytes = new long[5];
        int[] dmlRows = new int[5];

        // Phase 1 — validation + markdown extract. Failures here surface as per-row outcomes
        // without ever issuing SQL, matching the "REST 200 with results[i].ok=false" contract
        // the bulk-import endpoint promises.
        for (int i = 0; i < n; i++) {
            Map<String, Expr> args = entityArgsList.get(i);
            try {
                String entityType = stringArg(args, "entity_type");
                com.starrocks.context.policy.CollectionTypePolicy.check(col.getCollectionType(), entityType);

                String entityKey = validateEntityKey(stringArg(args, "entity_key"));
                if (!Strings.isNullOrEmpty(entityKey) && !seenKeysInBatch.add(entityKey)) {
                    outcomes[i] = UpsertOutcome.failure(i,
                            "duplicate entity_key in batch: " + entityKey);
                    continue;
                }
                String title = stringArg(args, "title");
                String providedPreview = stringArg(args, "preview");
                String rawBody = orDefault(stringArg(args, "content"), "");
                double confidence = doubleArg(args, "confidence", 1.0);

                MarkdownExtractor.Extracted extracted =
                        MarkdownExtractor.extract(rawBody, providedPreview);
                String body = extracted.body;
                String preview = extracted.preview;
                if (Strings.isNullOrEmpty(preview)) {
                    preview = truncate(body, PREVIEW_MAX_CHARS);
                }

                long givenId = longArg(args, "id", -1L);
                if (givenId < 0L) {
                    givenId = longArg(args, "entity_id", -1L);
                }

                Prepared p = new Prepared();
                p.entityKey = entityKey;
                p.entityType = entityType;
                p.title = title;
                p.preview = preview;
                p.body = body;
                p.rawBody = rawBody;
                p.extracted = extracted;
                p.confidence = confidence;
                p.givenId = givenId;
                prepared[i] = p;

                if (givenId < 0L && !Strings.isNullOrEmpty(entityKey)) {
                    keysToResolve.add(entityKey);
                }

                List<Expr> edges = (perEntityEdges != null && i < perEntityEdges.size())
                        ? perEntityEdges.get(i) : null;
                if (edges != null) {
                    for (Expr edge : edges) {
                        if (edge instanceof StringLiteral) {
                            String k = ((StringLiteral) edge).getValue();
                            if (!Strings.isNullOrEmpty(k)) {
                                distinctEdgeKeys.add(k);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                outcomes[i] = UpsertOutcome.failure(i, e.getMessage());
            }
        }
        long p1 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();

        // Phase 2 — bulk lookups. Two SELECTs replace 2N point lookups in the per-row path.
        // If the lookup itself fails (storage flake, SQL transient), we MUST fail every row whose
        // entity_key resolution is still pending rather than fall through to fresh-id allocation
        // for them — see the rationale in the single-row path above. The earlier safeResolveKeys
        // helper returned Collections.emptyMap() on failure, which caused Phase 3 to allocate a
        // brand-new entity_id for each entity that already existed, forking history.
        com.starrocks.context.ContextReadExecutor reader =
                com.starrocks.server.GlobalStateMgr.getCurrentState().getContextReadExecutor();
        Map<String, Long> entityKeyMap;
        try {
            entityKeyMap = keysToResolve.isEmpty() ? Collections.emptyMap()
                    : reader.resolveEntityIdsByKeys(keysToResolve, cb.getId(), col.getId());
        } catch (Exception e) {
            LOG.warn("upsertBatch entity_key lookup failed; marking pre-validated rows as failed", e);
            String msg = "entity_key lookup failed: " + e.getMessage();
            for (int i = 0; i < n; i++) {
                if (outcomes[i] == null && prepared[i] != null
                        && prepared[i].givenId < 0L && !Strings.isNullOrEmpty(prepared[i].entityKey)) {
                    outcomes[i] = UpsertOutcome.failure(i, msg);
                }
            }
            entityKeyMap = Collections.emptyMap();
        }
        Map<String, Long> edgeKeyMap;
        try {
            edgeKeyMap = distinctEdgeKeys.isEmpty() ? Collections.emptyMap()
                    : reader.resolveEntityIdsByKeys(distinctEdgeKeys, cb.getId(), null);
        } catch (Exception e) {
            // Edge resolution failure is recoverable: explicit edges that can't be resolved are
            // dropped (per the existing resolveEdgeList contract for unknown keys), but we
            // surface the failure so operators see it instead of silently degrading every batch.
            LOG.warn("upsertBatch edge_key lookup failed; explicit edges in this batch will be dropped", e);
            edgeKeyMap = Collections.emptyMap();
        }
        long p2 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();
        LOG.info("upsertBatch phase2 entityKey_lookup_keys={} edgeKey_lookup_keys={} ms={}",
                keysToResolve.size(), distinctEdgeKeys.size(), p2);

        // Phase 3 — id allocation per row. Allocators are pure in-memory counters; the only SQL
        // lurking here is `ContextVersionAllocator.next()`'s lazy `SELECT MAX(version)` on first
        // touch of an entity_id. For fresh ids we just allocated via `ContextIdAllocator.next()`
        // we know nothing is in storage yet, so we explicitly `seed(id, 0)` and the next() call
        // becomes pure-memory increment — eliminating an N+1 that would otherwise dominate the
        // wall-clock for brand-new bulk imports.
        Set<Long> reusedIds = new LinkedHashSet<>();
        for (int i = 0; i < n; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            Prepared p = prepared[i];
            long entityId = p.givenId;
            boolean reused = false;
            if (entityId < 0L && !Strings.isNullOrEmpty(p.entityKey)) {
                Long resolved = entityKeyMap.get(p.entityKey);
                if (resolved != null && resolved > 0L) {
                    entityId = resolved;
                    reused = true;
                }
            } else if (entityId > 0L) {
                // Explicit id bypasses the scoped entity_key resolution, so verify it lives in
                // this (contextbase, collection) before appending a version. On a scope mismatch
                // fail just this row, not the whole batch.
                try {
                    currentRowInScope(entityId, cb.getId(), col.getId());
                } catch (Exception scopeErr) {
                    outcomes[i] = UpsertOutcome.failure(i, scopeErr.getMessage());
                    continue;
                }
                reused = true;
            }
            if (entityId < 0L) {
                entityId = ContextIdAllocator.next();
                versionAllocator.seed(entityId, 0L);
            }
            p.entityId = entityId;
            if (reused) {
                reusedIds.add(entityId);
            }
        }
        long p3 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();

        // Phase 4 — bulk fetch created_time only for reused ids; new ids start with `now`.
        Map<Long, String> createdTimeMap = reusedIds.isEmpty()
                ? Collections.emptyMap() : reader.loadCreatedTimes(reusedIds);

        // One bulk import is one logical commit: every surviving row shares a single
        // snapshot_version so the whole batch is an atomic point on the snapshot timeline
        // (an as-of fence either includes all rows or none) and only one context_commits row
        // is written. Allocated lazily off the first surviving row, so a fully-invalid batch
        // burns no snapshot number. `version` stays per-entity (versionAllocator).
        long batchSnapshot = -1L;
        for (int i = 0; i < n; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            Prepared p = prepared[i];
            p.version = versionAllocator.next(p.entityId);
            if (batchSnapshot < 0L) {
                batchSnapshot = snapshotAllocator.next();
            }
            p.snapshotVersion = batchSnapshot;
            String created = createdTimeMap.get(p.entityId);
            p.createdTime = Strings.isNullOrEmpty(created) ? now : created;
            // Pre-resolve explicit edges to (dst, kind) tuples so phase-5 ref-row construction is
            // pure string assembly.
            List<Expr> rawEdges = (perEntityEdges != null && i < perEntityEdges.size())
                    ? perEntityEdges.get(i) : null;
            p.resolvedExplicitEdges = resolveEdgeList(rawEdges, edgeKeyMap);
        }
        long p4 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();
        LOG.info("upsertBatch phase4 reused_ids={} createdTime_lookup_ms={}", reusedIds.size(), p4);

        // Phase 4.5 — resolve markdown ref entity_keys ([[e:smb_baseline]] / source: [foo])
        // against a single live-heads lookup, then per-row check that every key can be resolved
        // either against that lookup or against the in-batch map (forward refs within this
        // batch). Rows with any unresolved key are failed here with ENTITY_NOT_FOUND so Phase 5
        // never builds a partial refs payload.
        Set<String> distinctRefKeys = new LinkedHashSet<>();
        for (int i = 0; i < n; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            distinctRefKeys.addAll(collectRefKeys(prepared[i].extracted));
        }
        Map<String, Long> liveRefKeyMap;
        try {
            liveRefKeyMap = distinctRefKeys.isEmpty() ? Collections.emptyMap()
                    : reader.resolveLiveEntityIdsByKeys(distinctRefKeys, cb.getId(), null);
        } catch (Exception e) {
            // Bulk lookup failure is recoverable per-row: anything resolvable from inBatchMap
            // alone still goes through; everything else fails with ENTITY_NOT_FOUND below.
            LOG.warn("upsertBatch ref-key live lookup failed; rows depending on heads will fail", e);
            liveRefKeyMap = Collections.emptyMap();
        }
        Map<String, Long> inBatchKeyMap = new HashMap<>();
        for (int i = 0; i < n; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            Prepared p = prepared[i];
            if (!Strings.isNullOrEmpty(p.entityKey)) {
                inBatchKeyMap.put(p.entityKey, p.entityId);
            }
        }
        for (int i = 0; i < n; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            Prepared p = prepared[i];
            List<String> unresolved = new ArrayList<>();
            p.refKeyMap = mergeRefKeyResolution(p.extracted, liveRefKeyMap, inBatchKeyMap, unresolved);
            if (!unresolved.isEmpty()) {
                outcomes[i] = UpsertOutcome.failure(i,
                        "unresolved entity_key references in contextbase '" + cb.getName() + "': "
                                + unresolved);
            }
        }
        long p45 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();
        LOG.info("upsertBatch phase4_5 ref_keys={} live_lookup_ms={}", distinctRefKeys.size(), p45);

        // Phase 4.7 — FE-side batch embedding precomputation. Collect every (entity, fragment)
        // text in input order, call the provider once, fan results back to each Prepared. The
        // fragments INSERT then writes array literals instead of `embedding(text, parse_json(cfg))`,
        // skipping the BE-side per-row HTTP fanout that previously dominated bulk-import latency.
        List<String> embedTexts = new ArrayList<>();
        List<int[]> embedSlots = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            Prepared p = prepared[i];
            if (!Strings.isNullOrEmpty(p.preview) && !p.preview.trim().isEmpty()) {
                embedSlots.add(new int[] {i, -1});
                embedTexts.add(p.preview);
            }
            int sectionCount = p.extracted.sections.size();
            p.sectionVectors = new float[sectionCount][];
            for (int s = 0; s < sectionCount; s++) {
                MarkdownExtractor.Section sec = p.extracted.sections.get(s);
                if (Strings.isNullOrEmpty(sec.text) || sec.text.trim().isEmpty()) {
                    continue;
                }
                embedSlots.add(new int[] {i, s});
                embedTexts.add(sec.text);
            }
        }
        long feEmbedHttpMs = 0;
        if (!embedTexts.isEmpty()) {
            long t = System.nanoTime();
            List<float[]> vectors = FeEmbeddingClient.embedBatch(embedTexts);
            feEmbedHttpMs = (System.nanoTime() - t) / 1_000_000;
            for (int k = 0; k < embedSlots.size(); k++) {
                int[] slot = embedSlots.get(k);
                Prepared p = prepared[slot[0]];
                if (slot[1] < 0) {
                    p.previewVector = vectors.get(k);
                } else {
                    p.sectionVectors[slot[1]] = vectors.get(k);
                }
            }
        }
        long p47 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();
        LOG.info("upsertBatch phase4_7 fe_embed_texts={} fe_embed_http_ms={} phase_ms={}",
                embedTexts.size(), feEmbedHttpMs, p47);

        // Phase 5 — build the multi-row INSERTs. tombstone=false for the public batch path; the
        // single-row tombstone helper still uses upsert(...) for its bookkeeping.
        SimpleExecutor executor = SimpleExecutor.getRepoExecutor();
        try {
            executeBatchedInserts(executor, prepared, outcomes, cb.getId(), col.getId(),
                    col.getCollectionType(), now, requestId, batchSnapshot,
                    dmlMs, dmlBytes, dmlRows);
        } catch (Exception sqlError) {
            String msg = "batched upsert SQL failed: " + sqlError.getMessage();
            // executeBatchedInserts optimistically marks every surviving row ok=true in its
            // row-build loop BEFORE running the 5 publish DMLs (versions, fragments stream-load,
            // refs, commits, heads). If any later step throws, those rows' writes are partial
            // (e.g. versions landed, then the fragments stream-load was rejected) yet their
            // outcomes still say success and their allocated keys are orphaned. Convert every
            // optimistic success back to a failure and best-effort DELETE its keys; leave genuine
            // pre-validation failures (outcomes[i] != null && !ok) untouched — nothing was written
            // for them.
            for (int i = 0; i < n; i++) {
                if (outcomes[i] != null && !outcomes[i].ok) {
                    continue;
                }
                Prepared p = prepared[i];
                if (p != null) {
                    cleanupOrphansBestEffort(executor, p.entityId, p.version, p.snapshotVersion);
                }
                outcomes[i] = UpsertOutcome.failure(i, msg);
            }
            LOG.warn("upsertBatch SQL phase failed; marked {} rows as failed",
                    countNullsThenFilled(outcomes), sqlError);
            return java.util.Arrays.asList(outcomes);
        }
        long p5 = swP.elapsed(TimeUnit.MILLISECONDS);
        swP.reset().start();
        LOG.info("upsertBatch phase5 ms={} dml_v_ms={} dml_f_ms={} dml_r_ms={} dml_c_ms={} dml_h_ms={}",
                p5, dmlMs[0], dmlMs[1], dmlMs[2], dmlMs[3], dmlMs[4]);
        LOG.info("upsertBatch phase5 bytes v={} f={} r={} c={} h={} rows v={} f={} r={} c={} h={}",
                dmlBytes[0], dmlBytes[1], dmlBytes[2], dmlBytes[3], dmlBytes[4],
                dmlRows[0], dmlRows[1], dmlRows[2], dmlRows[3], dmlRows[4]);

        // Phase 6 — metrics + audit. Log per-row so the audit shape matches the single-row path
        // (one line per upsert) — important for grep-based audit pipelines.
        long successCount = 0;
        for (UpsertOutcome o : outcomes) {
            if (o != null && o.ok) {
                successCount++;
            }
        }
        if (successCount > 0) {
            MetricRepo.COUNTER_CONTEXT_UPSERT_TOTAL.increase(successCount);
        }
        for (int i = 0; i < n; i++) {
            UpsertOutcome o = outcomes[i];
            if (o == null || !o.ok) {
                continue;
            }
            Prepared p = prepared[i];
            AuditLog.getInternalAudit().info(
                    "context_upsert | collection={} | entity_id={} version={} snapshot_version={} "
                            + "entity_key={} request_id={}",
                    collection, p.entityId, p.version, p.snapshotVersion,
                    p.entityKey == null ? "" : p.entityKey,
                    requestId == null ? "" : requestId);
        }
        long embedBytes = 0;
        int frags = 0;
        for (Prepared pr : prepared) {
            if (pr == null || pr.extracted == null) {
                continue;
            }
            frags += 1 + pr.extracted.sections.size();
            embedBytes += pr.preview == null ? 0 : pr.preview.length();
            for (MarkdownExtractor.Section s : pr.extracted.sections) {
                embedBytes += s.text == null ? 0 : s.text.length();
            }
        }
        LOG.info("upsertBatch ok collection={} n={} ok={} fail={} frags={} embed_bytes={} total_ms={}",
                collection, n, successCount, n - successCount, frags, embedBytes,
                swBatch.elapsed(TimeUnit.MILLISECONDS));
        LOG.info("upsertBatch phases p1={} p2={} p3={} p4={} p45={} p47={} p5={}",
                p1, p2, p3, p4, p45, p47, p5);
        return java.util.Arrays.asList(outcomes);
    }

    /** Carrier for one input row's pre-validated state. Mutated in-place across phases. */
    private static final class Prepared {
        String entityKey;
        String entityType;
        String title;
        String preview;
        String body;
        String rawBody;
        MarkdownExtractor.Extracted extracted;
        double confidence;
        long givenId;
        long entityId;
        long version;
        long snapshotVersion;
        String createdTime;
        List<ResolvedEdge> resolvedExplicitEdges; // explicit edges (kind constant 'explicit')
        // Per-row entity_key → dst_entity_id map. Populated in Phase 4.5 with the merged result
        // of (in-batch map + live heads map). Phase 5 substitutes via this when building refs
        // rows. Rows whose refs can't all resolve never reach Phase 5: their outcome is failed
        // with ENTITY_NOT_FOUND in Phase 4.5 itself.
        Map<String, Long> refKeyMap;
        // PERF — Phase 4.7 stash. previewVector aligns with the preview fragment; sectionVectors
        // is parallel to extracted.sections (null for sections whose text is empty/whitespace).
        float[] previewVector;
        float[][] sectionVectors;
    }

    /** One explicit edge resolved for the batch write path: numeric id (0 = unresolved) + the
     *  destination key it came from (null for numeric edges). Mirrors the single-upsert path:
     *  forward-reference-safe — a key that can't be resolved yet is kept with dstId=0 and resolved
     *  against heads at read time by {@code ReferenceExpander}. */
    private static final class ResolvedEdge {
        final long dstId;
        final String dstKey;

        ResolvedEdge(long dstId, String dstKey) {
            this.dstId = dstId;
            this.dstKey = dstKey;
        }
    }

    private static List<ResolvedEdge> resolveEdgeList(List<Expr> edges, Map<String, Long> edgeKeyMap) {
        if (edges == null || edges.isEmpty()) {
            return Collections.emptyList();
        }
        List<ResolvedEdge> out = new ArrayList<>(edges.size());
        for (Expr edge : edges) {
            if (edge instanceof IntLiteral) {
                // Numeric edge: id only, no key to fall back on.
                out.add(new ResolvedEdge(((IntLiteral) edge).getLongValue(), null));
            } else if (edge instanceof StringLiteral) {
                String k = ((StringLiteral) edge).getValue();
                if (Strings.isNullOrEmpty(k)) {
                    LOG.warn("upsertBatch: dropping malformed edge (empty key) {}", edge);
                    continue;
                }
                Long id = edgeKeyMap.get(k);
                // Forward-reference-safe: keep the edge even when the key is not yet resolvable
                // (cross-batch / not in heads). Store dstId=0 + key; ReferenceExpander resolves the
                // key against heads at read time.
                out.add(new ResolvedEdge(id != null ? id : 0L, k));
            } else {
                LOG.warn("upsertBatch: dropping malformed edge (no id, no key) {}", edge);
            }
        }
        return out;
    }

    private void executeBatchedInserts(SimpleExecutor executor, Prepared[] prepared,
                                       UpsertOutcome[] outcomes, long contextBaseId,
                                       long collectionId, String collectionType, String now,
                                       String requestId, long batchSnapshot,
                                       long[] dmlMs, long[] dmlBytes, int[] dmlRows) {
        StringBuilder versionsBuf = new StringBuilder();
        StringBuilder headsBuf = new StringBuilder();
        StringBuilder refsBuf = new StringBuilder();
        StringBuilder commitsBuf = new StringBuilder();
        boolean anyV = false;
        boolean anyH = false;
        boolean anyR = false;
        boolean anyC = false;

        for (int i = 0; i < prepared.length; i++) {
            if (outcomes[i] != null) {
                continue;
            }
            Prepared p = prepared[i];
            anyV = appendVersionsRow(versionsBuf, anyV, p, contextBaseId, collectionId,
                    collectionType, requestId, now);
            anyH = appendHeadsRow(headsBuf, anyH, p, contextBaseId, collectionId, collectionType, now);
            anyR = appendRefsRows(refsBuf, anyR, p);
            outcomes[i] = UpsertOutcome.success(i, new UpsertResult(
                    p.entityId, p.version, p.snapshotVersion, p.entityKey));
        }
        // One commit row for the whole batch — every surviving row shares batchSnapshot, so a
        // per-row append would emit N duplicate-PK tuples into the snapshot_version-keyed
        // context_commits table. Build it once after the row loop.
        if (anyV) {
            anyC = appendCommitsRow(commitsBuf, anyC, batchSnapshot, contextBaseId, requestId, now);
        }
        // Same publish-last ordering as the single-row path: versions/fragments/refs/commits
        // are written before heads, so a failure anywhere upstream leaves readers seeing the
        // previous heads state (or no row, on first insert) rather than a heads row pointing
        // at incomplete state.
        int okRows = 0;
        for (UpsertOutcome o : outcomes) {
            if (o != null && o.ok) {
                okRows++;
            }
        }
        for (int i = 0; i < 5; i++) {
            dmlRows[i] = okRows;
        }
        if (anyV) {
            String s = versionsBuf.toString();
            dmlBytes[0] = s.length();
            long t = System.nanoTime();
            executor.executeDML(s);
            dmlMs[0] = (System.nanoTime() - t) / 1_000_000;
        }
        // Fragments go through Stream Load (FE→BE HTTP PUT) instead of INSERT … VALUES. The
        // INSERT path scaled badly on this table because every row carries a 1536-dim
        // ARRAY<FLOAT> embedding literal: a 274-row batch produced ~6.5 MB of SQL and ~420 K
        // NumericLiteral tokens, which spent the bulk of dml_f_ms in ANTLR + analyzer + planner
        // before BE ever saw a row. Stream Load JSON carries the same rows as a 1.4 MB JSON
        // array and lets BE skip the entire FE SQL pipeline.
        int fragmentRowCount = countFragmentRows(prepared, outcomes);
        if (fragmentRowCount > 0) {
            String jsonBody = buildFragmentsJsonBatch(prepared, outcomes, contextBaseId, collectionId);
            dmlBytes[1] = jsonBody.length();
            dmlRows[1] = fragmentRowCount;
            long t = System.nanoTime();
            StreamLoader loader = new StreamLoader(
                    ContextInternalTables.DATABASE,
                    ContextInternalTables.FRAGMENTS,
                    FRAGMENTS_STREAM_LOAD_COLUMNS);
            StreamLoader.Response resp;
            try {
                resp = loader.loadBatch(streamLoadLabel(requestId), jsonBody);
            } catch (Exception e) {
                throw new RuntimeException("fragments stream load failed: " + e.getMessage(), e);
            }
            if (resp.status() != HttpStatus.SC_OK) {
                throw new RuntimeException("fragments stream load rejected: " + resp.msg());
            }
            dmlMs[1] = (System.nanoTime() - t) / 1_000_000;
        }
        if (anyR) {
            String s = refsBuf.toString();
            dmlBytes[2] = s.length();
            long t = System.nanoTime();
            executor.executeDML(s);
            dmlMs[2] = (System.nanoTime() - t) / 1_000_000;
        }
        if (anyC) {
            String s = commitsBuf.toString();
            dmlBytes[3] = s.length();
            long t = System.nanoTime();
            executor.executeDML(s);
            dmlMs[3] = (System.nanoTime() - t) / 1_000_000;
        }
        if (anyH) {
            String s = headsBuf.toString();
            dmlBytes[4] = s.length();
            long t = System.nanoTime();
            executor.executeDML(s);
            dmlMs[4] = (System.nanoTime() - t) / 1_000_000;
        }
    }

    private static boolean appendVersionsRow(StringBuilder buf, boolean any, Prepared p,
                                             long contextBaseId, long collectionId,
                                             String collectionType, String requestId, String now) {
        if (!any) {
            buf.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.VERSIONS)
                    .append(" (entity_id, version, entity_key, contextbase_id, collection_id, "
                            + "collection_type, entity_type, status, title, preview, body, "
                            + "raw_markdown, frontmatter_json, source_json, confidence, "
                            + "body_token_count, created_time, updated_time, commit_time, "
                            + "snapshot_version, request_id, deleted) VALUES ");
        } else {
            buf.append(',');
        }
        buf.append('(').append(p.entityId).append(',').append(p.version).append(',')
                .append(sqlStringOrNull(p.entityKey)).append(',')
                .append(contextBaseId).append(',').append(collectionId).append(',')
                .append(sqlString(collectionType)).append(',').append(sqlString(p.entityType))
                .append(",'ACTIVE',").append(sqlStringOrNull(p.title)).append(',')
                .append(sqlString(p.preview)).append(',').append(sqlString(p.body)).append(',')
                .append(sqlString(p.rawBody)).append(',')
                .append(sqlStringOrNull(p.extracted.frontmatterJson)).append(',')
                .append(sqlStringOrNull(p.extracted.sourceJson)).append(',')
                .append(p.confidence).append(',').append(p.body.length()).append(",'")
                .append(p.createdTime).append("','").append(now).append("','").append(now)
                .append("',").append(p.snapshotVersion).append(',')
                .append(sqlStringOrNull(requestId)).append(",false)");
        return true;
    }

    private static boolean appendHeadsRow(StringBuilder buf, boolean any, Prepared p,
                                          long contextBaseId, long collectionId,
                                          String collectionType, String now) {
        if (!any) {
            buf.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.HEADS)
                    .append(" (entity_id, entity_key, contextbase_id, collection_id, "
                            + "collection_type, entity_type, current_version, "
                            + "current_snapshot_version, current_preview, current_confidence, "
                            + "current_updated_time, current_deleted, "
                            + "last_ref_version, last_fragment_version) VALUES ");
        } else {
            buf.append(',');
        }
        buf.append('(').append(p.entityId).append(',').append(sqlStringOrNull(p.entityKey))
                .append(',').append(contextBaseId).append(',').append(collectionId).append(',')
                .append(sqlString(collectionType)).append(',').append(sqlString(p.entityType))
                .append(',').append(p.version).append(',').append(p.snapshotVersion).append(',')
                .append(sqlString(p.preview)).append(',').append(p.confidence).append(",'")
                .append(now).append("',false,")
                .append(p.version).append(',').append(p.version)
                .append(')');
        return true;
    }

    private static final List<String> FRAGMENTS_STREAM_LOAD_COLUMNS = List.of(
            "entity_id", "version", "fragment_id", "fragment_kind", "ordinal",
            "line_start", "line_end", "fragment_preview", "fragment_text",
            "token_count", "embedding", "snapshot_version", "contextbase_id", "collection_id");

    private static int countFragmentRows(Prepared[] prepared, UpsertOutcome[] outcomes) {
        int n = 0;
        for (int i = 0; i < prepared.length; i++) {
            // Skip pre-validation failures (outcomes[i] != null && !ok). The success
            // outcomes have already been written into outcomes[i] by executeBatchedInserts'
            // main row-build loop, so the gate is "outcomes[i] == null || outcomes[i].ok".
            if (outcomes[i] != null && !outcomes[i].ok) {
                continue;
            }
            Prepared p = prepared[i];
            if (p == null) {
                continue;
            }
            if (!Strings.isNullOrEmpty(p.preview) && !p.preview.trim().isEmpty()) {
                n++;
            }
            for (MarkdownExtractor.Section section : p.extracted.sections) {
                if (!Strings.isNullOrEmpty(section.text) && !section.text.trim().isEmpty()) {
                    n++;
                }
            }
        }
        return n;
    }

    /**
     * Serialize the fragments rows as a JSON array suitable for the BE {@code _stream_load}
     * endpoint with {@code strip_outer_array=true}. Mirrors the original
     * {@code INSERT INTO context_entity_fragments (...) VALUES (...)} row order one-for-one;
     * skips empty-text fragments so the HNSW index never sees a zero-length embedding row.
     */
    private static String buildFragmentsJsonBatch(Prepared[] prepared, UpsertOutcome[] outcomes,
                                                  long contextBaseId, long collectionId) {
        StringWriter sw = new StringWriter();
        try (JsonWriter w = new JsonWriter(sw)) {
            w.beginArray();
            for (int i = 0; i < prepared.length; i++) {
                if (outcomes[i] != null && !outcomes[i].ok) {
                    continue;
                }
                Prepared p = prepared[i];
                if (p == null) {
                    continue;
                }
                long fragmentId = 0;
                if (!Strings.isNullOrEmpty(p.preview) && !p.preview.trim().isEmpty()) {
                    writeFragmentRow(w, p, fragmentId++, "preview", 0, 1, 1,
                            p.preview, p.preview, p.previewVector, contextBaseId, collectionId);
                }
                for (int s = 0; s < p.extracted.sections.size(); s++) {
                    MarkdownExtractor.Section section = p.extracted.sections.get(s);
                    if (Strings.isNullOrEmpty(section.text) || section.text.trim().isEmpty()) {
                        continue;
                    }
                    writeFragmentRow(w, p, fragmentId++, "section", section.ordinal,
                            section.lineStart, section.lineEnd, section.preview, section.text,
                            p.sectionVectors[s], contextBaseId, collectionId);
                }
            }
            w.endArray();
        } catch (IOException e) {
            throw new RuntimeException("fragments JSON serialization failed: " + e.getMessage(), e);
        }
        return sw.toString();
    }

    private static void writeFragmentRow(JsonWriter w, Prepared p, long fragmentId,
                                         String fragmentKind, int ordinal,
                                         int lineStart, int lineEnd, String fragmentPreview,
                                         String fragmentText, float[] embedding,
                                         long contextBaseId, long collectionId) throws IOException {
        w.beginObject();
        w.name("entity_id").value(p.entityId);
        w.name("version").value(p.version);
        w.name("fragment_id").value(fragmentId);
        w.name("fragment_kind").value(fragmentKind);
        w.name("ordinal").value(ordinal);
        w.name("line_start").value(lineStart);
        w.name("line_end").value(lineEnd);
        w.name("fragment_preview").value(fragmentPreview == null ? "" : fragmentPreview);
        w.name("fragment_text").value(fragmentText);
        w.name("token_count").value(fragmentText.length());
        w.name("embedding");
        w.beginArray();
        if (embedding != null) {
            for (float v : embedding) {
                w.value(v);
            }
        }
        w.endArray();
        w.name("snapshot_version").value(p.snapshotVersion);
        w.name("contextbase_id").value(contextBaseId);
        w.name("collection_id").value(collectionId);
        w.endObject();
    }

    /** Build a per-call label for Stream Load. BE uses this for de-dup at the txn level. */
    private static String streamLoadLabel(String requestId) {
        String suffix = String.valueOf(System.nanoTime());
        if (Strings.isNullOrEmpty(requestId)) {
            return "ctx_frag_" + suffix;
        }
        return "ctx_frag_" + requestId.replace('-', '_') + "_" + suffix;
    }

    private static boolean appendRefsRows(StringBuilder buf, boolean any, Prepared p) {
        int ord = 0;
        for (MarkdownExtractor.InlineRef ref : p.extracted.inlineRefs) {
            long dst = ref.dstEntityId != null ? ref.dstEntityId : p.refKeyMap.get(ref.dstEntityKey);
            // inline/source refs always resolve to a real id (strict path) — no key fallback.
            any = appendOneRefRow(buf, any, p, ord++, dst, "inline", null);
        }
        for (MarkdownExtractor.RefToken t : p.extracted.sourceRefs) {
            long dst = t.id != null ? t.id : p.refKeyMap.get(t.key);
            any = appendOneRefRow(buf, any, p, ord++, dst, "source", null);
        }
        if (p.resolvedExplicitEdges != null) {
            for (ResolvedEdge edge : p.resolvedExplicitEdges) {
                // Explicit edges carry dst_entity_key so unresolved (dstId=0) forward refs resolve
                // by key at read time.
                any = appendOneRefRow(buf, any, p, ord++, edge.dstId, "explicit", edge.dstKey);
            }
        }
        return any;
    }

    private static boolean appendOneRefRow(StringBuilder buf, boolean any, Prepared p, int ord,
                                           long dstEntityId, String refKind, String dstKey) {
        if (!any) {
            buf.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.REFS)
                    .append(" (src_entity_id, src_version, ord, dst_entity_id, ref_kind, "
                            + "ref_label, snapshot_version, dst_entity_key) VALUES ");
        } else {
            buf.append(',');
        }
        buf.append('(').append(p.entityId).append(',').append(p.version).append(',').append(ord)
                .append(',').append(dstEntityId).append(',').append(sqlString(refKind))
                .append(",NULL,").append(p.snapshotVersion).append(',')
                .append(Strings.isNullOrEmpty(dstKey) ? "NULL" : sqlString(dstKey)).append(')');
        return true;
    }

    private static boolean appendCommitsRow(StringBuilder buf, boolean any, long snapshotVersion,
                                            long contextBaseId, String requestId, String now) {
        if (!any) {
            buf.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.COMMITS)
                    .append(" (snapshot_version, contextbase_id, request_id, commit_time, "
                            + "visibility_state, primary_ready, refs_ready, fragments_ready, "
                            + "error_message) VALUES ");
        } else {
            buf.append(',');
        }
        buf.append('(').append(snapshotVersion).append(',').append(contextBaseId).append(',')
                .append(sqlStringOrNull(requestId)).append(",'").append(now).append("',")
                .append("'VISIBLE',true,true,true,NULL)");
        return true;
    }

    private static long countNullsThenFilled(UpsertOutcome[] outcomes) {
        long n = 0;
        for (UpsertOutcome o : outcomes) {
            if (o != null && !o.ok) {
                n++;
            }
        }
        return n;
    }

    /**
     * Cross-scope guard shared by the explicit-id write/delete paths. context_entity_heads is
     * keyed by entity_id alone, and entity_id is a global sequence shared across every
     * contextbase — so an id resolved or supplied for one (contextbase, collection) must never be
     * written to or deleted under another, or it would append a version to / destroy a different
     * tenant's entity. Loads the id's current row; throws {@link IllegalStateException} (opaque
     * "entity not found", so the error cannot probe cross-base existence) when the row lives in a
     * different scope. Returns the row, or {@code null} when the id has no current version yet
     * (a fresh explicit id on the upsert path is allowed to create one).
     */
    private ContextReadExecutor.VersionRow currentRowInScope(long entityId, long contextBaseId,
                                                             long collectionId) {
        ContextReadExecutor.VersionRow row = com.starrocks.server.GlobalStateMgr.getCurrentState()
                .getContextReadExecutor().loadCurrentVersionRow(entityId);
        if (row != null && (row.contextBaseId != contextBaseId || row.collectionId != collectionId)) {
            throw new IllegalStateException("entity not found: " + entityId);
        }
        return row;
    }

    public UpsertResult hardDelete(ContextCollectionName collection, long entityId, String entityKey,
                                   Map<String, Expr> options) {
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(collection.getContextBase());
        if (cb == null) {
            throw new IllegalStateException("contextbase not found: " + collection.getContextBase());
        }
        ContextMgr.CollectionMeta col = findCollection(collection);
        if (col == null) {
            throw new IllegalStateException("collection not found: " + collection);
        }
        // The DELETEs below key on entityId alone. Reject a scope mismatch first, otherwise an id
        // that resolves to a different (contextbase, collection) would have its
        // refs/fragments/versions/heads wiped while the tombstone commit is recorded under the
        // requested contextbase (updateMetadata / the batch upsert path apply the same guard).
        currentRowInScope(entityId, cb.getId(), col.getId());
        String requestId = stringArg(options, "request_id");
        long snapshotVersion = snapshotAllocator.next();
        String now = TS_FMT.format(LocalDateTime.now());
        SimpleExecutor executor = SimpleExecutor.getRepoExecutor();

        executor.executeDML(String.format(
                "DELETE FROM %s.%s WHERE src_entity_id = %d OR dst_entity_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.REFS, entityId, entityId));
        executor.executeDML(String.format(
                "DELETE FROM %s.%s WHERE entity_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.FRAGMENTS, entityId));
        executor.executeDML(String.format(
                "DELETE FROM %s.%s WHERE entity_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId));
        executor.executeDML(String.format(
                "DELETE FROM %s.%s WHERE entity_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS, entityId));

        String commitsInsert = String.format(
                "INSERT INTO %s.%s ("
                        + "snapshot_version, contextbase_id, request_id, commit_time, "
                        + "visibility_state, primary_ready, refs_ready, fragments_ready, error_message) "
                        + "VALUES (%d, %d, %s, '%s', 'VISIBLE', true, true, true, NULL)",
                ContextInternalTables.DATABASE, ContextInternalTables.COMMITS,
                snapshotVersion, cb.getId(), sqlStringOrNull(requestId), now);
        executor.executeDML(commitsInsert);
        MetricRepo.COUNTER_CONTEXT_DELETE_TOTAL.increase(1L);
        return new UpsertResult(entityId, 0L, snapshotVersion, entityKey);
    }

    /**
     * Look up the current head's {@code entity_type} for the given entity. Returns null when the
     * head doesn't exist or the lookup fails — caller decides whether to treat that as fatal.
     */
    private String lookupCurrentEntityType(long entityId) {
        try {
            String sql = String.format(
                    "SELECT entity_type FROM %s.%s WHERE entity_id = %d LIMIT 1",
                    ContextInternalTables.DATABASE, ContextInternalTables.HEADS, entityId);
            java.util.List<com.starrocks.thrift.TResultBatch> batches =
                    SimpleExecutor.getRepoExecutor().executeDQL(sql);
            for (com.starrocks.thrift.TResultBatch batch : batches) {
                if (batch.getRows() == null) {
                    continue;
                }
                for (java.nio.ByteBuffer buf : batch.getRows()) {
                    io.netty.buffer.ByteBuf copied = io.netty.buffer.Unpooled.copiedBuffer(buf);
                    com.google.gson.JsonElement parsed = com.google.gson.JsonParser.parseString(
                            copied.toString(java.nio.charset.Charset.defaultCharset()));
                    com.google.gson.JsonArray data = parsed.getAsJsonObject().getAsJsonArray("data");
                    if (data.size() > 0 && !data.get(0).isJsonNull()) {
                        return data.get(0).getAsString();
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("entity_type lookup failed for tombstone: {}", e.getMessage());
        }
        return null;
    }

    private String lookupCurrentCreatedTime(long entityId) {
        try {
            String sql = String.format(
                    "SELECT created_time FROM %s.%s WHERE entity_id = %d ORDER BY version ASC LIMIT 1",
                    ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId);
            java.util.List<com.starrocks.thrift.TResultBatch> batches =
                    SimpleExecutor.getRepoExecutor().executeDQL(sql);
            for (com.starrocks.thrift.TResultBatch batch : batches) {
                if (batch.getRows() == null) {
                    continue;
                }
                for (java.nio.ByteBuffer buf : batch.getRows()) {
                    io.netty.buffer.ByteBuf copied = io.netty.buffer.Unpooled.copiedBuffer(buf);
                    com.google.gson.JsonElement parsed = com.google.gson.JsonParser.parseString(
                            copied.toString(java.nio.charset.Charset.defaultCharset()));
                    com.google.gson.JsonArray data = parsed.getAsJsonObject().getAsJsonArray("data");
                    if (data.size() > 0 && !data.get(0).isJsonNull()) {
                        return data.get(0).getAsString();
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("created_time lookup failed for entity {}: {}", entityId, e.getMessage());
        }
        return null;
    }

    private ContextMgr.CollectionMeta findCollection(ContextCollectionName name) {
        for (ContextMgr.CollectionMeta m : contextMgr.listCollections(name.getContextBase())) {
            if (m.getName().equals(name.getCollection())) {
                return m;
            }
        }
        return null;
    }

    /**
     * Best-effort compensating cleanup of versions / fragments / refs / commits rows that may
     * have been partially written before a publish-chain failure. Each DELETE is wrapped in its
     * own try/catch so a failure in one stage doesn't abort the cleanup of the others. Cleanup
     * runs against the (entity_id, version) and (snapshot_version) keys this upsert allocated,
     * so it can't collide with rows from a concurrent upsert on the same entity (which would
     * have a different version) or a concurrent upsert on another entity (different entity_id).
     */
    private void cleanupOrphansBestEffort(SimpleExecutor executor, long entityId, long version, long snapshotVersion) {
        String[] deletes = new String[] {
                String.format("DELETE FROM %s.%s WHERE entity_id = %d AND version = %d",
                        ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId, version),
                String.format("DELETE FROM %s.%s WHERE src_entity_id = %d AND src_version = %d",
                        ContextInternalTables.DATABASE, ContextInternalTables.REFS, entityId, version),
                String.format("DELETE FROM %s.%s WHERE entity_id = %d AND version = %d",
                        ContextInternalTables.DATABASE, ContextInternalTables.FRAGMENTS, entityId, version),
                String.format("DELETE FROM %s.%s WHERE snapshot_version = %d",
                        ContextInternalTables.DATABASE, ContextInternalTables.COMMITS, snapshotVersion),
        };
        for (String sql : deletes) {
            try {
                executor.executeDML(sql);
            } catch (Exception e) {
                LOG.warn("compensating cleanup failed: {} | error: {}", sql, e.getMessage());
            }
        }
    }

    private void writeFragments(SimpleExecutor executor, long entityId, long version, long snapshotVersion,
                                long contextBaseId, long collectionId,
                                String preview, MarkdownExtractor.Extracted extracted) {
        // Every fragment row carries a real embedding computed by the BE-side
        // `embedding(text, parse_json(config))` scalar function during the INSERT. The per-fragment
        // HTTP fanout runs on BE workers (one batched OpenAI request per chunk) and lands the
        // vector in the row, so VectorSearchExecutor's approx_cosine_similarity(query, f.embedding)
        // clause can scan immediately.
        //
        // Empty-text fragments (preview="" or extracted section.text="") are skipped here so the
        // HNSW index never sees zero-length entries.
        String embeddingConfigJson = buildEmbeddingConfigJson();

        String insertColumns = " (entity_id, version, fragment_id, fragment_kind, ordinal, line_start, line_end, "
                + "fragment_preview, fragment_text, token_count, embedding, snapshot_version"
                + ", contextbase_id, collection_id) VALUES ";
        String scopeSuffix = String.format(", %d, %d)", contextBaseId, collectionId);

        StringBuilder buf = new StringBuilder();
        boolean any = false;
        long fragmentId = 0;
        if (!Strings.isNullOrEmpty(preview) && !preview.trim().isEmpty()) {
            String previewEmbeddingExpr = embeddingExpression(embeddingConfigJson, preview);
            buf.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.FRAGMENTS).append(insertColumns);
            buf.append(String.format(
                    "(%d, %d, %d, 'preview', 0, 1, 1, %s, %s, %d, %s, %d",
                    entityId, version, fragmentId++,
                    sqlString(preview), sqlString(preview), (long) preview.length(),
                    previewEmbeddingExpr, snapshotVersion)).append(scopeSuffix);
            any = true;
        }
        for (MarkdownExtractor.Section section : extracted.sections) {
            if (Strings.isNullOrEmpty(section.text) || section.text.trim().isEmpty()) {
                continue;
            }
            String sectionEmbeddingExpr = embeddingExpression(embeddingConfigJson, section.text);
            if (!any) {
                buf.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                        .append(ContextInternalTables.FRAGMENTS).append(insertColumns);
            } else {
                buf.append(", ");
            }
            buf.append(String.format(
                    "(%d, %d, %d, 'section', %d, %d, %d, %s, %s, %d, %s, %d",
                    entityId, version, fragmentId++, section.ordinal,
                    section.lineStart, section.lineEnd,
                    sqlString(section.preview), sqlString(section.text),
                    (long) section.text.length(),
                    sectionEmbeddingExpr, snapshotVersion)).append(scopeSuffix);
            any = true;
        }
        if (any) {
            executor.executeDML(buf.toString());
        }
    }

    /**
     * Delegates to {@link com.starrocks.context.embedding.EmbeddingConfigJson#build()} so every FE
     * caller produces an identical, security-audited JSON shape (API key referenced by env-var
     * name, never by value). Kept as a method here so existing tests don't have to import the
     * embedding package.
     */
    static String buildEmbeddingConfigJson() {
        return com.starrocks.context.embedding.EmbeddingConfigJson.build();
    }

    /**
     * Render the SQL expression that produces the embedding column value for a row's INSERT. The
     * provider must be configured — callers that receive a {@code null} configJson should fail
     * earlier with a clearer error; this method enforces the invariant defensively. Empty text
     * still returns the {@code "[]"} sentinel for the (NOT NULL) column; the writer is expected
     * to filter empty-text fragments upstream, so this branch is only reached for sentinel rows
     * the schema requires (e.g. an entity with an empty preview but non-empty body sections).
     */
    static String embeddingExpression(String configJson, String text) {
        if (configJson == null) {
            throw new SemanticException("CONTEXT UPSERT requires a DEFAULT EMBEDDING PROVIDER: "
                    + "run CREATE EMBEDDING PROVIDER ...; SET <name> AS DEFAULT EMBEDDING PROVIDER");
        }
        if (Strings.isNullOrEmpty(text)) {
            return "[]";
        }
        return "embedding(" + sqlString(text) + ", parse_json(" + sqlString(configJson) + "))";
    }

    private void writeRefs(SimpleExecutor executor, long entityId, long version, long snapshotVersion,
                           long contextBaseId, String contextBaseName,
                           MarkdownExtractor.Extracted extracted,
                           java.util.List<Expr> edges) {
        // Resolve every entity_key referenced from body or frontmatter against live entities in
        // this contextbase. Numeric refs ([[e:231]] / source: [201]) are left untouched and write
        // straight through. Strict mode: any unresolved key aborts the upsert before we touch
        // SQL, so we never persist a partial refs set.
        Set<String> keys = collectRefKeys(extracted);
        Map<String, Long> liveHeadsMap;
        try {
            liveHeadsMap = keys.isEmpty() ? Collections.emptyMap()
                    : com.starrocks.server.GlobalStateMgr.getCurrentState()
                            .getContextReadExecutor()
                            .resolveLiveEntityIdsByKeys(keys, contextBaseId, /*collectionId=*/ null);
        } catch (Exception e) {
            throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                    "entity_key reference lookup failed: " + e.getMessage());
        }
        List<String> unresolved = new ArrayList<>();
        Map<String, Long> resolved = mergeRefKeyResolution(extracted, liveHeadsMap,
                /*inBatchKeyMap=*/ null, unresolved);
        if (!unresolved.isEmpty()) {
            throw new ContextException(ContextErrorCode.ENTITY_NOT_FOUND,
                    "unresolved entity_key references in contextbase '" + contextBaseName + "': "
                            + unresolved);
        }

        // Collect every ref row first, then issue ONE multi-row INSERT — same pattern the
        // batch path uses. The previous per-ref executeDML produced O(N) round-trips for an
        // entity with N inline refs + sources + explicit edges.
        StringBuilder valuesBuf = new StringBuilder();
        int rowCount = 0;
        int ord = 0;
        for (MarkdownExtractor.InlineRef ref : extracted.inlineRefs) {
            long dst = ref.dstEntityId != null ? ref.dstEntityId : resolved.get(ref.dstEntityKey);
            if (rowCount++ > 0) {
                valuesBuf.append(", ");
            }
            // inline/source refs always resolve to a real id (strict path above), so dst_entity_key
            // is NULL — only explicit edges use the key fallback.
            valuesBuf.append(String.format(
                    "(%d, %d, %d, %d, 'inline', NULL, %d, NULL)",
                    entityId, version, ord++, dst, snapshotVersion));
        }
        for (MarkdownExtractor.RefToken t : extracted.sourceRefs) {
            long dst = t.id != null ? t.id : resolved.get(t.key);
            if (rowCount++ > 0) {
                valuesBuf.append(", ");
            }
            valuesBuf.append(String.format(
                    "(%d, %d, %d, %d, 'source', NULL, %d, NULL)",
                    entityId, version, ord++, dst, snapshotVersion));
        }
        // Explicit EDGES (...) from the SQL / REST surface are persisted as ref_kind='explicit'.
        // Forward-reference-safe (RDF/KG style): an edge given as an entity_key always stores that
        // key. If the target already exists we also store its resolved id (fast path); if not, we
        // store dst_entity_id=0 ("unresolved") and let ReferenceExpander resolve dst_entity_key ->
        // heads at read time. Only a truly malformed edge (no id and no key) is dropped.
        if (edges != null) {
            for (Expr edge : edges) {
                String dstKey = edge instanceof StringLiteral ? ((StringLiteral) edge).getValue() : null;
                Long dst = resolveEdgeDst(edge, contextBaseId);
                long dstId;
                String keyLiteral;
                if (dst != null) {
                    dstId = dst;
                    keyLiteral = Strings.isNullOrEmpty(dstKey) ? "NULL" : sqlString(dstKey);
                } else if (!Strings.isNullOrEmpty(dstKey)) {
                    dstId = 0L;                 // unresolved forward ref — keep, resolve by key at read
                    keyLiteral = sqlString(dstKey);
                } else {
                    LOG.warn("CONTEXT UPSERT EDGES: dropping malformed edge (no id, no key) {}", edge);
                    continue;
                }
                if (rowCount++ > 0) {
                    valuesBuf.append(", ");
                }
                valuesBuf.append(String.format(
                        "(%d, %d, %d, %d, 'explicit', NULL, %d, %s)",
                        entityId, version, ord++, dstId, snapshotVersion, keyLiteral));
            }
        }
        if (rowCount == 0) {
            return;
        }
        String sql = "INSERT INTO " + ContextInternalTables.DATABASE + "." + ContextInternalTables.REFS
                + " (src_entity_id, src_version, ord, dst_entity_id, ref_kind, ref_label, "
                + "snapshot_version, dst_entity_key) "
                + "VALUES " + valuesBuf;
        executor.executeDML(sql);
    }

    private Long resolveEdgeDst(Expr edge, long contextBaseId) {
        if (edge instanceof com.starrocks.sql.ast.expression.IntLiteral) {
            return ((com.starrocks.sql.ast.expression.IntLiteral) edge).getLongValue();
        }
        if (edge instanceof StringLiteral) {
            String key = ((StringLiteral) edge).getValue();
            if (Strings.isNullOrEmpty(key)) {
                return null;
            }
            try {
                // Edges may cross collections within the same base but never cross bases — pin
                // the lookup to this entity's contextbase so a key collision in a sibling base
                // can't produce a stray edge into someone else's data.
                long resolved = com.starrocks.server.GlobalStateMgr.getCurrentState()
                        .getContextReadExecutor()
                        .resolveEntityIdByKey(key, contextBaseId, /*collectionId=*/ null);
                return resolved > 0L ? resolved : null;
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Reject digit-only entity_key at write time. See {@link #DIGIT_ONLY_ENTITY_KEY} for rationale.
     * Returns the input unchanged so callers can chain on it.
     */
    public static String validateEntityKey(String entityKey) {
        if (!Strings.isNullOrEmpty(entityKey) && DIGIT_ONLY_ENTITY_KEY.matcher(entityKey).matches()) {
            throw new ContextException(ContextErrorCode.INVALID_ENTITY_KEY,
                    "entity_key must contain at least one non-digit character; got '" + entityKey + "'");
        }
        return entityKey;
    }

    /**
     * Resolve every {@code entity_key} appearing in {@code extracted.inlineRefs} /
     * {@code extracted.sourceRefs} against live entities in the contextbase.
     *
     * <p>Resolution order, per row:
     * <ol>
     *   <li>If {@code inBatchKeyMap} is non-null and contains the key, use that id. This covers
     *       intra-batch forward references — A in row 1 cites B from row 2, both new — because
     *       Phase 3 of the batch path allocates ids before this resolver runs.</li>
     *   <li>Otherwise look the key up in {@code liveHeadsMap} (the result of
     *       {@code ContextReadExecutor.resolveLiveEntityIdsByKeys}, base-scoped, tombstones
     *       filtered out).</li>
     * </ol>
     *
     * <p>Returns a map containing every key that resolved; missing keys are reported via the
     * {@code unresolved} list (in input order, deduplicated). Numeric refs are never inspected
     * (they bypass resolution entirely and write straight through to the refs table).
     */
    public static Map<String, Long> mergeRefKeyResolution(MarkdownExtractor.Extracted extracted,
                                                          Map<String, Long> liveHeadsMap,
                                                          Map<String, Long> inBatchKeyMap,
                                                          List<String> unresolvedOut) {
        Map<String, Long> resolved = new HashMap<>();
        Set<String> reportedMissing = new LinkedHashSet<>();
        for (MarkdownExtractor.InlineRef r : extracted.inlineRefs) {
            if (r.dstEntityKey != null) {
                resolveOneKey(r.dstEntityKey, liveHeadsMap, inBatchKeyMap, resolved, reportedMissing);
            }
        }
        for (MarkdownExtractor.RefToken t : extracted.sourceRefs) {
            if (t.key != null) {
                resolveOneKey(t.key, liveHeadsMap, inBatchKeyMap, resolved, reportedMissing);
            }
        }
        if (unresolvedOut != null) {
            unresolvedOut.addAll(reportedMissing);
        }
        return resolved;
    }

    private static void resolveOneKey(String key, Map<String, Long> liveHeadsMap,
                                      Map<String, Long> inBatchKeyMap, Map<String, Long> resolved,
                                      Set<String> reportedMissing) {
        if (resolved.containsKey(key) || reportedMissing.contains(key)) {
            return;
        }
        if (inBatchKeyMap != null) {
            Long fromBatch = inBatchKeyMap.get(key);
            if (fromBatch != null && fromBatch > 0L) {
                resolved.put(key, fromBatch);
                return;
            }
        }
        Long fromHeads = liveHeadsMap == null ? null : liveHeadsMap.get(key);
        if (fromHeads != null && fromHeads > 0L) {
            resolved.put(key, fromHeads);
            return;
        }
        reportedMissing.add(key);
    }

    /**
     * Collect every distinct {@code entity_key} referenced by the body / frontmatter of
     * {@code extracted}. Used by the write path to build a single batched
     * {@code resolveLiveEntityIdsByKeys} lookup before SQL emission.
     */
    public static Set<String> collectRefKeys(MarkdownExtractor.Extracted extracted) {
        Set<String> out = new LinkedHashSet<>();
        for (MarkdownExtractor.InlineRef r : extracted.inlineRefs) {
            if (r.dstEntityKey != null) {
                out.add(r.dstEntityKey);
            }
        }
        for (MarkdownExtractor.RefToken t : extracted.sourceRefs) {
            if (t.key != null) {
                out.add(t.key);
            }
        }
        return out;
    }

    private static String stringArg(Map<String, Expr> args, String key) {
        if (args == null) {
            return null;
        }
        Expr expr = args.get(key);
        if (expr == null) {
            return null;
        }
        if (expr instanceof StringLiteral) {
            return ((StringLiteral) expr).getValue();
        }
        if (expr instanceof LiteralExpr) {
            // FloatLiteral / IntLiteral / BoolLiteral all override getStringValue() to return the
            // canonical literal text (e.g. "0.0", "16341", "true"). Expr.debugString() on the
            // other hand returns "()" for leaf literals because it joins children with no value
            // for the literal itself — that silently broke doubleArg("confidence") parsing and
            // dropped the confidence=0.0 signal in the soft-delete path.
            return ((LiteralExpr) expr).getStringValue();
        }
        return expr.toString();
    }

    private static double doubleArg(Map<String, Expr> args, String key, double defaultValue) {
        String s = stringArg(args, key);
        if (Strings.isNullOrEmpty(s)) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long longArg(Map<String, Expr> args, String key, long defaultValue) {
        String s = stringArg(args, key);
        if (Strings.isNullOrEmpty(s)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String orDefault(String value, String fallback) {
        return Strings.isNullOrEmpty(value) ? fallback : value;
    }

    static String sqlString(String s) {
        // Forwarded to ContextSqlEscape so the four context-module write paths share a single
        // hardened implementation (NUL drop + control-char encoding). See ContextSqlEscape for
        // why the earlier ad-hoc {@code replace("\\", "\\\\").replace("'", "''")} pair was
        // unsafe for user-supplied markdown bodies.
        return ContextSqlEscape.literal(s);
    }

    private static String sqlStringOrNull(String s) {
        return ContextSqlEscape.literalOrNull(s);
    }

    private static String truncate(String s, int maxChars) {
        if (s == null) {
            return "";
        }
        return s.length() <= maxChars ? s : s.substring(0, maxChars);
    }
}
