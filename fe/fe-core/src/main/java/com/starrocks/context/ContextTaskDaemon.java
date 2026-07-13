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
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.context.policy.CollectionTypePolicy;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic task processor for the background-task families:
 * {@code BULK_IMPORT}, {@code DERIVED_PAGE}, {@code REFERENCE_RESYNC},
 * {@code WORKSPACE_COMMIT}. Each instance polls {@link ContextInternalTables#TASKS} for one
 * specific {@code task_type}, claims pending rows by transitioning them to {@code RUNNING},
 * runs a kind-specific handler, and transitions to {@code COMPLETED} or {@code FAILED}.
 *
 * <p>All four kinds are real handlers now: bulk import upserts each entity from the payload;
 * derived-page generates a summary entity with citations back to its sources; reference resync
 * re-extracts inline `[[e:id]]` refs from a target version's body and rewrites
 * {@code context_entity_refs}; workspace commit promotes every non-tombstoned workspace object
 * into the named target collection. Per-row failures are isolated within a task and surface via
 * the {@code SHOW CONTEXT TASKS} stream.
 */
public class ContextTaskDaemon extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(ContextTaskDaemon.class);
    // Outer cycle interval. Kept small (1s) only to pace non-leader follower threads — when this
    // FE is the leader, the real wait happens inside {@code runAfterCatalogReady} via
    // {@code wakeQueue.poll(WAKE_POLL_TIMEOUT_SEC, SECONDS)}. The previous value (60s) was the
    // sole source of task-dispatch latency: a newly submitted task waited 0–60s for the next
    // tick, which surfaced as the 320s runtime of {@code test_semantic_context_workspace} (two
    // WORKSPACE_COMMIT tasks each blocking ~160s on their daemon's polling cycle).
    private static final long RUN_INTERVAL_MS = 1_000L;
    private static final long WAKE_POLL_TIMEOUT_SEC = 60L;
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int BATCH_SIZE = 25;
    private static final Gson GSON = new Gson();
    private static final String DEFAULT_MEMORY_COLLECTION_PROPERTY = "default_memory_collection";
    private static final String DEFAULT_TASK_SUMMARY_COLLECTION_PROPERTY = "default_task_summary_collection";
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    private final ContextTaskScheduler.TaskKind kind;
    // Per-process identifier used in lease tokens; new value every FE start, so a stale leader
    // can never recognise its own pre-failover lease.
    private static final String FE_INSTANCE_ID = UUID.randomUUID().toString();
    private final AtomicLong leaseCounter = new AtomicLong();

    public ContextTaskDaemon(ContextTaskScheduler.TaskKind kind) {
        super("context-" + kind.name().toLowerCase() + "-daemon", RUN_INTERVAL_MS);
        this.kind = kind;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        // Event-driven dispatch: block up to WAKE_POLL_TIMEOUT_SEC for a wake signal from
        // ContextTaskScheduler.submit (offered into the per-kind wake queue). On timeout we still
        // fall through to processPending — this is the safety scan that catches:
        //   * tasks submitted on a follower (no offer fired this leader's queue)
        //   * tasks that failed handle() and need a retry
        //   * any wake-signal lost across FE failover
        // After waking we drain any extra signals — multiple submits in quick succession only
        // need to trigger one processPending tick, which already batches up to BATCH_SIZE rows.
        BlockingQueue<Long> wakeQueue =
                GlobalStateMgr.getCurrentState().getContextTaskScheduler().wakeQueueFor(kind);
        try {
            wakeQueue.poll(WAKE_POLL_TIMEOUT_SEC, TimeUnit.SECONDS);
            wakeQueue.clear();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            processPending();
        } catch (Exception e) {
            LOG.warn("{} iteration failed", getName(), e);
        }
    }

    private void processPending() {
        String pickSql = String.format(
                "SELECT task_id, contextbase_id, payload_json FROM %s.%s "
                        + "WHERE task_type = '%s' AND state = 'PENDING' "
                        + "ORDER BY created_time ASC LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS,
                kind.name(), BATCH_SIZE);
        JsonArray rows = runQuery(pickSql);
        // Collect terminal-state writes and flush as a single UPDATE at end of tick. Per-task
        // UPDATE used to issue 2-3 round-trips per task (claim/complete/fail); now claim still
        // costs one CAS-verify pair but the terminal write is amortized across the tick.
        List<Long> completed = new ArrayList<>();
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            long taskId = data.get(0).getAsLong();
            long contextBaseId = data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsLong() : 0L;
            String payload = data.size() > 2 ? jsonColumnToString(data.get(2)) : null;
            // CAS-claim the task before handle(): conditional UPDATE state=PENDING→RUNNING
            // writing a unique lease into error_message, then SELECT to confirm the lease.
            // If another worker (e.g. a stale leader during FE failover) claimed first we
            // skip handle() entirely — otherwise the non-idempotent handlers (bulk-import,
            // workspace commit) would run twice on the same row.
            if (!claimRunning(taskId)) {
                LOG.info("{} task {} not claimed (already running/completed elsewhere)", getName(), taskId);
                continue;
            }
            try {
                handle(taskId, contextBaseId, payload);
                completed.add(taskId);
            } catch (Exception e) {
                LOG.warn("{} task {} failed: {}", getName(), taskId, e.getMessage(), e);
                // Failed tasks carry per-row error text so a single combined UPDATE isn't
                // straightforward — fall back to per-task UPDATE. Failures are rare so the
                // extra round-trip is acceptable.
                markFailed(taskId, e.getMessage());
            }
        }
        if (!completed.isEmpty()) {
            flushCompleted(completed);
        }
    }

    private void flushCompleted(List<Long> taskIds) {
        StringBuilder inList = new StringBuilder();
        for (int i = 0; i < taskIds.size(); i++) {
            if (i > 0) {
                inList.append(',');
            }
            inList.append(taskIds.get(i));
        }
        String now = TS_FMT.format(LocalDateTime.now());
        String sql = String.format(
                "UPDATE %s.%s SET state = 'COMPLETED', updated_time = '%s', error_message = NULL "
                        + "WHERE task_id IN (%s)",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS, now, inList);
        try {
            SimpleExecutor.getRepoExecutor().executeDML(sql);
        } catch (Exception e) {
            // Surface but don't fail the tick — the lease tokens will recycle naturally as the
            // next iteration re-evaluates state.
            LOG.warn("{} flushCompleted failed for {} tasks: {}", getName(), taskIds.size(), e.getMessage());
        }
    }

    /**
     * Kind-specific handler. Subclasses can override; the default is a no-op transition that just
     * moves the row from {@code RUNNING} to {@code COMPLETED} so operators see the lifecycle even
     * when the real pipeline isn't built. This intentionally errs on the side of "completed" —
     * a never-completing task pile would mask real issues; a quickly-completed task is at least
     * a clear signal that the daemon is alive and the pipeline is the next thing to build.
     */
    protected void handle(long taskId, long contextBaseId, String payload) throws Exception {
        switch (kind) {
            case BULK_IMPORT:
                handleBulkImport(taskId, contextBaseId, payload);
                break;
            case DERIVED_PAGE:
                handleDerivedPage(taskId, contextBaseId, payload);
                break;
            case REFERENCE_RESYNC:
                handleReferenceResync(taskId, contextBaseId, payload);
                break;
            case WORKSPACE_COMMIT:
                handleWorkspaceCommit(taskId, contextBaseId, payload);
                break;
        }
    }

    /**
     * Bulk import: process a payload of the form
     * <pre>{@code
     * {
     *   "contextbase": "sales_ai",
     *   "collection": "pipeline_rules",
     *   "options": { ... },
     *   "entities": [ {...entity...}, ... ]
     * }
     * }</pre>
     * For each entity, calls {@link ContextWriteExecutor#upsert} — same path the REST endpoint
     * uses, so semantics match. Per-row failures are isolated; the task is marked
     * {@code COMPLETED} as long as the payload itself was well-formed and at least one entity
     * was processed (success or failure). A wholly malformed payload fails the task.
     */
    protected void handleBulkImport(long taskId, long contextBaseId, String payload) {
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException("BULK_IMPORT task " + taskId + " has empty payload");
        }
        Map<String, Object> p = GSON.fromJson(payload, MAP_TYPE);
        String contextBase = (String) p.get("contextbase");
        String collection = (String) p.get("collection");
        if (contextBase == null || collection == null) {
            throw new IllegalArgumentException(
                    "BULK_IMPORT payload missing 'contextbase' or 'collection'");
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entities = (List<Map<String, Object>>) p.get("entities");
        if (entities == null || entities.isEmpty()) {
            throw new IllegalArgumentException(
                    "BULK_IMPORT payload has no entities to ingest");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> options = (Map<String, Object>) p.get("options");

        ContextCollectionName name =
                new ContextCollectionName(contextBase, collection, NodePosition.ZERO);
        ContextWriteExecutor writer =
                GlobalStateMgr.getCurrentState().getContextWriteExecutor();
        Map<String, Expr> optionExprs = toExprMap(options);

        // Use the batched upsert path: 5 multi-row INSERTs replace 5×N round-trips. Per-row
        // error isolation comes from the validate-then-execute pipeline inside upsertBatch.
        java.util.List<java.util.Map<String, Expr>> argsList = new java.util.ArrayList<>(entities.size());
        for (Map<String, Object> ent : entities) {
            argsList.add(toExprMap(ent));
        }
        java.util.List<ContextWriteExecutor.UpsertOutcome> outcomes =
                writer.upsertBatch(name, argsList, /*perEntityEdges*/ null, optionExprs);

        int ok = 0;
        int failed = 0;
        for (ContextWriteExecutor.UpsertOutcome o : outcomes) {
            if (o.ok) {
                ok++;
            } else {
                failed++;
                LOG.warn("BULK_IMPORT task {} entity #{} failed: {}", taskId, o.index, o.errorMessage);
            }
        }
        LOG.info("BULK_IMPORT task {} processed {} entities ({} ok, {} failed)",
                taskId, entities.size(), ok, failed);
        if (ok == 0 && failed > 0) {
            // Every row failed — surface that on the task so SHOW CONTEXT TASKS reflects it,
            // rather than reporting a successful no-op.
            throw new IllegalStateException(
                    "BULK_IMPORT task " + taskId + ": all " + failed
                            + " entities failed; check logs / per-row errors");
        }
    }

    private static Map<String, Expr> toExprMap(Map<String, Object> in) {
        if (in == null) {
            return null;
        }
        Map<String, Expr> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : in.entrySet()) {
            Object value = entry.getValue();
            String str = value == null ? null : value.toString();
            out.put(entry.getKey(), new StringLiteral(str == null ? "" : str));
        }
        return out;
    }

    /**
     * Derived-page generation. Payload shape:
     * <pre>{@code
     * {
     *   "contextbase":  "sales_ai",
     *   "collection":   "pipeline_rules",
     *   "source_entity_ids": [101, 102, ...],
     *   "derive_kind":  "summary",                 // freeform tag — recorded in the body
     *   "title":        "Pipeline rules digest",   // optional; default: "<derive_kind> derived page"
     *   "options":      { ... }
     * }
     * }</pre>
     * For each source entity, fetch the current head's preview, concatenate them with citations
     * back to the source ids (via the `[[e:id]]` ref syntax that {@code MarkdownExtractor} already
     * parses out), then upsert that as a new entity in the target collection. The new entity's
     * inline refs feed the existing reference-expansion path so callers can navigate from the
     * derived page back to its sources.
     */
    protected void handleDerivedPage(long taskId, long contextBaseId, String payload) {
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException("DERIVED_PAGE task " + taskId + " has empty payload");
        }
        Map<String, Object> p = GSON.fromJson(payload, MAP_TYPE);
        String contextBase = (String) p.get("contextbase");
        String collection = (String) p.get("collection");
        @SuppressWarnings("unchecked")
        List<Object> sourceIds = (List<Object>) p.get("source_entity_ids");
        if (contextBase == null || collection == null || sourceIds == null || sourceIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "DERIVED_PAGE payload requires contextbase, collection, source_entity_ids");
        }
        String deriveKind = (String) p.getOrDefault("derive_kind", "summary");
        String title = (String) p.getOrDefault("title", deriveKind + " derived page");
        @SuppressWarnings("unchecked")
        Map<String, Object> options = (Map<String, Object>) p.get("options");

        ContextReadExecutor reader = new ContextReadExecutor();
        StringBuilder body = new StringBuilder();
        body.append("---\n");
        body.append("derive_kind: ").append(deriveKind).append("\n");
        body.append("source_entity_ids: ");
        for (int i = 0; i < sourceIds.size(); i++) {
            if (i > 0) {
                body.append(", ");
            }
            body.append(numberLiteral(sourceIds.get(i)));
        }
        body.append("\n---\n\n");
        body.append("# ").append(title).append("\n\n");

        for (Object idObj : sourceIds) {
            long id = ((Number) idObj).longValue();
            JsonArray rows = reader.getCurrentById(id, ContextReadExecutor.DisclosureLevel.STANDARD);
            String preview = extractStringField(rows, /*PREVIEW idx*/ 4);
            String srcTitle = extractStringField(rows, /*TITLE idx*/ 8);
            body.append("## [[e:").append(id).append("]]");
            if (srcTitle != null && !srcTitle.isEmpty()) {
                body.append(" ").append(srcTitle);
            }
            body.append("\n\n");
            body.append(preview == null || preview.isEmpty() ? "(empty preview)" : preview);
            body.append("\n\n");
        }

        ContextCollectionName name =
                new ContextCollectionName(contextBase, collection, NodePosition.ZERO);
        ContextWriteExecutor writer =
                GlobalStateMgr.getCurrentState().getContextWriteExecutor();

        Map<String, Expr> entityArgs = new LinkedHashMap<>();
        entityArgs.put("entity_type", new StringLiteral("derived_page"));
        entityArgs.put("entity_key",
                new StringLiteral("derived/" + deriveKind + "/" + taskId));
        entityArgs.put("title", new StringLiteral(title));
        entityArgs.put("content", new StringLiteral(body.toString()));
        Map<String, Expr> optionExprs = toExprMap(options);

        ContextWriteExecutor.UpsertResult result = writer.upsert(name, entityArgs, optionExprs);
        LOG.info("DERIVED_PAGE task {} produced entity_id={} version={} from {} sources",
                taskId, result.entityId, result.version, sourceIds.size());
    }

    /**
     * Reference resync. Payload shape:
     * <pre>{@code
     * {
     *   "entity_id":  123,
     *   "version":    7              // optional; default: current head version
     * }
     * }</pre>
     * Re-extracts the inline `[[e:id]]` refs from the named entity's body and rewrites the
     * matching rows in {@code context_entity_refs}. This is the recovery path for cases where
     * (a) refs were lost due to a partially-failed write, or (b) a downstream entity was renamed
     * and the reference text needs re-resolving against the latest head map. The resync is
     * idempotent: it deletes the existing rows for the (entity_id, version) tuple and re-inserts
     * the freshly-extracted ones.
     */
    protected void handleReferenceResync(long taskId, long contextBaseId, String payload) {
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException(
                    "REFERENCE_RESYNC task " + taskId + " has empty payload");
        }
        Map<String, Object> p = GSON.fromJson(payload, MAP_TYPE);
        Object entityIdObj = p.get("entity_id");
        if (entityIdObj == null) {
            throw new IllegalArgumentException("REFERENCE_RESYNC payload requires entity_id");
        }
        long entityId = ((Number) entityIdObj).longValue();

        // Resolve the version: explicit if given, else read head.
        long version;
        Object versionObj = p.get("version");
        if (versionObj != null) {
            version = ((Number) versionObj).longValue();
        } else {
            JsonArray head = new ContextReadExecutor()
                    .getCurrentById(entityId, ContextReadExecutor.DisclosureLevel.PREVIEW);
            if (head.size() == 0) {
                throw new IllegalStateException("REFERENCE_RESYNC: entity " + entityId + " not found");
            }
            JsonArray data = head.get(0).getAsJsonObject().getAsJsonArray("data");
            version = data.get(1).getAsLong();   // current_version is column 1
        }

        // Fetch the body for the resolved (entity_id, version). contextBaseId came in via the
        // task payload (and ContextTaskScheduler row), so the entity_key resolution step below
        // can scope its IN-list to the contextbase the entity belongs to without an extra hop
        // back through versions.
        String selectBody = String.format(
                "SELECT body FROM %s.%s WHERE entity_id = %d AND version = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS, entityId, version);
        JsonArray bodyRows = runQuery(selectBody);
        if (bodyRows.size() == 0) {
            throw new IllegalStateException(
                    "REFERENCE_RESYNC: version " + version + " of entity " + entityId + " not found");
        }
        String body = bodyRows.get(0).getAsJsonObject().getAsJsonArray("data").get(0).getAsString();

        com.starrocks.context.markdown.MarkdownExtractor.Extracted extracted =
                com.starrocks.context.markdown.MarkdownExtractor.extract(body, null);

        // Resolve entity_key refs against live heads. Strict mode at write time blocks any
        // upsert with unresolvable keys, so when a resync produces unresolved keys here it means
        // a target entity was deleted after the citing entity was written. The previous snapshot
        // layer (refs with the original numeric ids it was written with) keeps serving reads;
        // the new snapshot just drops those rows. ord is reassigned over the rows that survive
        // so the (src, version, ord, snapshot_version) PK stays contiguous.
        java.util.Set<String> keys = ContextWriteExecutor.collectRefKeys(extracted);
        Map<String, Long> liveRefKeyMap;
        try {
            liveRefKeyMap = keys.isEmpty() ? java.util.Collections.emptyMap()
                    : new ContextReadExecutor().resolveLiveEntityIdsByKeys(keys, contextBaseId, null);
        } catch (Exception e) {
            LOG.warn("REFERENCE_RESYNC task {}: live entity_key lookup failed; dropping key refs in this snapshot",
                    taskId, e);
            liveRefKeyMap = java.util.Collections.emptyMap();
        }
        List<String> unresolvedKeys = new ArrayList<>();
        Map<String, Long> refKeyMap = ContextWriteExecutor.mergeRefKeyResolution(
                extracted, liveRefKeyMap, /*inBatchKeyMap=*/ null, unresolvedKeys);
        if (!unresolvedKeys.isEmpty()) {
            LOG.warn("REFERENCE_RESYNC task {}: dropping {} refs whose targets are not live "
                            + "(deleted or never created): {}",
                    taskId, unresolvedKeys.size(), unresolvedKeys);
        }

        // Append the freshly-extracted refs as a new (snapshot_version) layer over the existing
        // (src_entity_id, src_version, ord) entries. The previous DELETE-then-INSERT version
        // silently rewrote the historical edge set so any reader that pinned an older snapshot
        // would see the latest refs instead of the historical truth. Refs PK now includes
        // snapshot_version (see ContextInternalTables.REFS_DDL), so multiple resyncs coexist
        // and read paths fence by `MAX(snapshot_version) <= request.snapshotFence`. Idempotency
        // is now per-snapshot rather than per-(src, version, ord).
        long snapshotVersion = GlobalStateMgr.getCurrentState()
                .getContextSnapshotAllocator().next();

        // Multi-row INSERT instead of one-DML-per-ref. Refs is a primary-key table on
        // (src_entity_id, src_version, ord) so a single INSERT VALUES (..),(..),... batched
        // statement is the same write semantics with one round-trip; mirrors the multi-row
        // pattern in ContextWriteExecutor.executeBatchedInserts (L687–734).
        //
        // We iterate inlineRefs / sourceRefs (union of numeric and key shapes) and substitute
        // resolved ids from refKeyMap for key-bearing refs. Refs whose key didn't resolve are
        // skipped silently (already logged above as unresolved); ord is incremented only for
        // emitted rows so the (src, version, ord, snapshot_version) PK stays contiguous.
        StringBuilder ins = new StringBuilder();
        ins.append("INSERT INTO ").append(ContextInternalTables.DATABASE).append('.')
                .append(ContextInternalTables.REFS)
                .append(" (src_entity_id, src_version, ord, dst_entity_id, ref_kind, "
                        + "ref_label, snapshot_version) VALUES ");
        boolean first = true;
        int ord = 0;
        for (com.starrocks.context.markdown.MarkdownExtractor.InlineRef ref : extracted.inlineRefs) {
            Long dst = ref.dstEntityId != null ? ref.dstEntityId : refKeyMap.get(ref.dstEntityKey);
            if (dst == null) {
                continue;
            }
            if (!first) {
                ins.append(',');
            }
            first = false;
            ins.append('(').append(entityId).append(',').append(version).append(',')
                    .append(ord++).append(',').append(dst)
                    .append(",'inline',NULL,").append(snapshotVersion).append(')');
        }
        for (com.starrocks.context.markdown.MarkdownExtractor.RefToken t : extracted.sourceRefs) {
            Long dst = t.id != null ? t.id : refKeyMap.get(t.key);
            if (dst == null) {
                continue;
            }
            if (!first) {
                ins.append(',');
            }
            first = false;
            ins.append('(').append(entityId).append(',').append(version).append(',')
                    .append(ord++).append(',').append(dst)
                    .append(",'source',NULL,").append(snapshotVersion).append(')');
        }
        if (ord > 0) {
            SimpleExecutor.getRepoExecutor().executeDML(ins.toString());
        }
        LOG.info("REFERENCE_RESYNC task {} rewrote {} refs for entity_id={} version={}",
                taskId, ord, entityId, version);
    }

    /**
     * Workspace commit. Payload shape:
     * <pre>{@code
     * {
     *   "workspace":         "sales_ai.pipeline_rules.session_42",
     *   "target_collection": "sales_ai.session_history",
     *   "options":           { ... }
     * }
     * }</pre>
     * Latest workspace objects are grouped by {@code workspace_scope}: {@code output} objects are
     * promoted into a {@code task_summary} collection, {@code memory} objects are promoted into a
     * {@code memory} collection, and {@code scratch} objects are discarded. Successful commits
     * tombstone the workspace rows and remove the workspace metadata; failed promotions retain the
     * workspace so the task can be retried safely.
     */
    protected void handleWorkspaceCommit(long taskId, long contextBaseId, String payload) {
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException(
                    "WORKSPACE_COMMIT task " + taskId + " has empty payload");
        }
        Map<String, Object> p = GSON.fromJson(payload, MAP_TYPE);
        String workspaceName = (String) p.get("workspace");
        if (Strings.isNullOrEmpty(workspaceName)) {
            throw new IllegalArgumentException("WORKSPACE_COMMIT payload requires workspace");
        }
        String explicitTargetCollection = (String) p.get("target_collection");
        @SuppressWarnings("unchecked")
        Map<String, Object> options = (Map<String, Object>) p.get("options");

        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        ContextMgr.WorkspaceMeta workspace = mgr.getWorkspace(workspaceName);
        if (workspace == null) {
            throw new IllegalStateException("WORKSPACE_COMMIT: workspace not found: " + workspaceName);
        }

        List<WorkspaceObjectRow> latestRows = loadLatestWorkspaceObjects(workspace.getId());
        List<WorkspaceObjectRow> activeRows = new ArrayList<>();
        boolean hasMemory = false;
        boolean hasOutput = false;
        int scratchCount = 0;
        for (WorkspaceObjectRow row : latestRows) {
            if (row.deleted) {
                continue;
            }
            activeRows.add(row);
            if (WorkspaceObjectWriter.WORKSPACE_SCOPE_MEMORY.equals(row.workspaceScope)) {
                hasMemory = true;
            } else if (WorkspaceObjectWriter.WORKSPACE_SCOPE_OUTPUT.equals(row.workspaceScope)) {
                hasOutput = true;
            } else {
                scratchCount++;
            }
        }

        ContextMgr.CollectionMeta memoryTarget = hasMemory
                ? resolveWorkspaceCommitTarget(mgr, workspace, WorkspaceObjectWriter.WORKSPACE_SCOPE_MEMORY,
                explicitTargetCollection)
                : null;
        ContextMgr.CollectionMeta outputTarget = hasOutput
                ? resolveWorkspaceCommitTarget(mgr, workspace, WorkspaceObjectWriter.WORKSPACE_SCOPE_OUTPUT,
                explicitTargetCollection)
                : null;
        ContextCollectionName memoryTargetName = memoryTarget == null ? null : toCollectionName(mgr, memoryTarget);
        ContextCollectionName outputTargetName = outputTarget == null ? null : toCollectionName(mgr, outputTarget);

        ContextWriteExecutor writer = GlobalStateMgr.getCurrentState().getContextWriteExecutor();
        Map<String, Expr> optionExprs = toExprMap(options);

        // Group promotable rows by their target collection so each group can use a single
        // batched upsert. memory and output may route to different collections; both are
        // independent. Skip scratch rows entirely (they're just discarded on commit).
        List<WorkspaceObjectRow> memoryRows = new ArrayList<>();
        List<WorkspaceObjectRow> outputRows = new ArrayList<>();
        for (WorkspaceObjectRow row : activeRows) {
            if (WorkspaceObjectWriter.WORKSPACE_SCOPE_MEMORY.equals(row.workspaceScope)) {
                memoryRows.add(row);
            } else if (WorkspaceObjectWriter.WORKSPACE_SCOPE_OUTPUT.equals(row.workspaceScope)) {
                outputRows.add(row);
            }
        }

        int promoted = 0;
        List<String> failures = new ArrayList<>();
        promoted += commitWorkspaceGroup(writer, taskId, workspaceName, memoryTargetName,
                memoryRows, optionExprs, failures);
        promoted += commitWorkspaceGroup(writer, taskId, workspaceName, outputTargetName,
                outputRows, optionExprs, failures);
        if (!failures.isEmpty()) {
            throw new IllegalStateException(
                    "WORKSPACE_COMMIT task " + taskId + " failed for " + failures.size()
                            + " object(s); workspace retained: " + String.join("; ", failures));
        }

        cleanupWorkspace(workspace, activeRows);
        LOG.info("WORKSPACE_COMMIT task {} workspace={} promoted {} object(s), discarded {} scratch object(s), "
                        + "memory_target={}, output_target={}",
                taskId, workspaceName, promoted, scratchCount,
                memoryTarget == null ? null : qualifiedCollectionName(mgr, memoryTarget),
                outputTarget == null ? null : qualifiedCollectionName(mgr, outputTarget));
    }

    /**
     * Promote one scope's rows to {@code target} via a single batched upsert. Returns the count
     * of rows that landed; failed rows are appended to {@code failures} so the daemon can decide
     * whether to retry the workspace as a whole.
     */
    private int commitWorkspaceGroup(ContextWriteExecutor writer, long taskId, String workspaceName,
                                     ContextCollectionName target, List<WorkspaceObjectRow> rows,
                                     Map<String, Expr> optionExprs, List<String> failures) {
        if (rows.isEmpty() || target == null) {
            return 0;
        }
        List<Map<String, Expr>> argsList = new ArrayList<>(rows.size());
        for (WorkspaceObjectRow row : rows) {
            String payloadJson = row.payloadJson == null ? "{}" : row.payloadJson;
            Map<String, Expr> entityArgs = new LinkedHashMap<>();
            entityArgs.put("entity_type", new StringLiteral("page"));
            entityArgs.put("entity_key",
                    new StringLiteral("workspace/" + workspaceName + "/" + row.objectId));
            entityArgs.put("content", new StringLiteral(extractContent(payloadJson)));
            argsList.add(entityArgs);
        }
        List<ContextWriteExecutor.UpsertOutcome> outcomes;
        try {
            outcomes = writer.upsertBatch(target, argsList, /*perEntityEdges*/ null, optionExprs);
        } catch (Exception e) {
            LOG.warn("WORKSPACE_COMMIT task {} batched upsert failed for target={}: {}",
                    taskId, target, e.getMessage());
            for (WorkspaceObjectRow row : rows) {
                failures.add(row.objectId + "[" + row.workspaceScope + "]: " + e.getMessage());
            }
            return 0;
        }
        int promoted = 0;
        for (int i = 0; i < outcomes.size(); i++) {
            ContextWriteExecutor.UpsertOutcome o = outcomes.get(i);
            if (o.ok) {
                promoted++;
            } else {
                WorkspaceObjectRow row = rows.get(i);
                LOG.warn("WORKSPACE_COMMIT task {} object_id={} scope={} failed: {}",
                        taskId, row.objectId, row.workspaceScope, o.errorMessage);
                failures.add(row.objectId + "[" + row.workspaceScope + "]: " + o.errorMessage);
            }
        }
        return promoted;
    }

    private void cleanupWorkspace(ContextMgr.WorkspaceMeta workspace, List<WorkspaceObjectRow> activeRows) {
        WorkspaceObjectWriter workspaceWriter = GlobalStateMgr.getCurrentState().getWorkspaceObjectWriter();
        for (WorkspaceObjectRow row : activeRows) {
            workspaceWriter.discard(workspace, row.objectId, row.workspaceScope);
        }
        GlobalStateMgr.getCurrentState().getContextMgr().dropWorkspace(workspace.getName(), true);
    }

    private ContextMgr.CollectionMeta resolveWorkspaceCommitTarget(ContextMgr mgr,
                                                                   ContextMgr.WorkspaceMeta workspace,
                                                                   String workspaceScope,
                                                                   String explicitTargetCollection) {
        String contextBaseName = workspaceContextBase(workspace.getName());
        if (Strings.isNullOrEmpty(contextBaseName)) {
            throw new IllegalStateException("WORKSPACE_COMMIT workspace name is not qualified: " + workspace.getName());
        }
        ContextMgr.ContextBaseMeta contextBase = mgr.getContextBase(contextBaseName);
        if (contextBase == null) {
            throw new IllegalStateException("WORKSPACE_COMMIT contextbase not found: " + contextBaseName);
        }
        ContextMgr.CollectionMeta workspaceCollection = mgr.resolveWorkspaceCollection(workspace);

        String propertyKey;
        String expectedCollectionType;
        if (WorkspaceObjectWriter.WORKSPACE_SCOPE_MEMORY.equals(workspaceScope)) {
            propertyKey = DEFAULT_MEMORY_COLLECTION_PROPERTY;
            expectedCollectionType = CollectionTypePolicy.TYPE_MEMORY;
        } else if (WorkspaceObjectWriter.WORKSPACE_SCOPE_OUTPUT.equals(workspaceScope)) {
            propertyKey = DEFAULT_TASK_SUMMARY_COLLECTION_PROPERTY;
            expectedCollectionType = CollectionTypePolicy.TYPE_TASK_SUMMARY;
        } else {
            throw new IllegalArgumentException("unsupported workspace scope for commit: " + workspaceScope);
        }

        String configuredTarget = configuredWorkspaceRoute(workspace, workspaceCollection, contextBase, propertyKey);
        if (!Strings.isNullOrEmpty(configuredTarget)) {
            return resolveTargetCollectionReference(mgr, contextBaseName, configuredTarget,
                    expectedCollectionType, propertyKey, false, workspaceScope);
        }

        List<ContextMgr.CollectionMeta> candidates = findCollectionsByType(mgr, contextBaseName, expectedCollectionType);
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        if (!Strings.isNullOrEmpty(explicitTargetCollection)) {
            return resolveTargetCollectionReference(mgr, contextBaseName, explicitTargetCollection,
                    expectedCollectionType, "target_collection", true, workspaceScope);
        }
        if (candidates.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "WORKSPACE_COMMIT route for scope '%s' is unresolved; no %s collection found in contextbase %s. "
                            + "Set %s on workspace/collection/contextbase or pass target_collection.",
                    workspaceScope, expectedCollectionType, contextBaseName, propertyKey));
        }
        throw new IllegalStateException(String.format(
                "WORKSPACE_COMMIT route for scope '%s' is ambiguous; found %s collections in contextbase %s: %s. "
                        + "Set %s on workspace/collection/contextbase or pass target_collection.",
                workspaceScope, expectedCollectionType, contextBaseName,
                qualifiedCollectionNames(mgr, candidates), propertyKey));
    }

    private ContextMgr.CollectionMeta resolveTargetCollectionReference(ContextMgr mgr,
                                                                       String defaultContextBase,
                                                                       String reference,
                                                                       String expectedCollectionType,
                                                                       String sourceLabel,
                                                                       boolean allowCrossContextBase,
                                                                       String workspaceScope) {
        String trimmed = reference == null ? null : reference.trim();
        if (Strings.isNullOrEmpty(trimmed)) {
            throw new IllegalStateException("WORKSPACE_COMMIT " + sourceLabel + " is empty");
        }
        String contextBaseName = defaultContextBase;
        String collectionName = trimmed;
        int dot = trimmed.indexOf('.');
        if (dot >= 0) {
            if (dot == 0 || dot == trimmed.length() - 1) {
                throw new IllegalStateException(
                        "WORKSPACE_COMMIT " + sourceLabel
                                + " must be of the form <collection> or <contextbase>.<collection>: " + reference);
            }
            contextBaseName = trimmed.substring(0, dot);
            collectionName = trimmed.substring(dot + 1);
        }
        if (!allowCrossContextBase && !defaultContextBase.equals(contextBaseName)) {
            throw new IllegalStateException(
                    "WORKSPACE_COMMIT " + sourceLabel + " must stay within contextbase "
                            + defaultContextBase + ": " + reference);
        }
        ContextMgr.CollectionMeta collection = mgr.getCollection(contextBaseName, collectionName);
        if (collection == null) {
            throw new IllegalStateException(
                    "WORKSPACE_COMMIT " + sourceLabel + " does not resolve to an existing collection: " + reference);
        }
        if (collection.getCollectionType() == null
                || !expectedCollectionType.equalsIgnoreCase(collection.getCollectionType())) {
            throw new IllegalStateException(String.format(
                    "WORKSPACE_COMMIT route for scope '%s' must point to a %s collection, but %s resolved to %s (%s)",
                    workspaceScope, expectedCollectionType, sourceLabel,
                    qualifiedCollectionName(mgr, collection), collection.getCollectionType()));
        }
        return collection;
    }

    private List<ContextMgr.CollectionMeta> findCollectionsByType(ContextMgr mgr, String contextBaseName,
                                                                  String collectionType) {
        List<ContextMgr.CollectionMeta> matches = new ArrayList<>();
        for (ContextMgr.CollectionMeta collection : mgr.listCollections(contextBaseName)) {
            if (collectionType.equalsIgnoreCase(collection.getCollectionType())) {
                matches.add(collection);
            }
        }
        return matches;
    }

    private ContextCollectionName toCollectionName(ContextMgr mgr, ContextMgr.CollectionMeta collection) {
        ContextMgr.ContextBaseMeta contextBase = mgr.getContextBaseById(collection.getContextBaseId());
        if (contextBase == null) {
            throw new IllegalStateException(
                    "WORKSPACE_COMMIT contextbase missing for collection id " + collection.getId());
        }
        return new ContextCollectionName(contextBase.getName(), collection.getName(), NodePosition.ZERO);
    }

    private String qualifiedCollectionName(ContextMgr mgr, ContextMgr.CollectionMeta collection) {
        ContextMgr.ContextBaseMeta contextBase = mgr.getContextBaseById(collection.getContextBaseId());
        return contextBase == null ? collection.getName() : contextBase.getName() + "." + collection.getName();
    }

    private String qualifiedCollectionNames(ContextMgr mgr, List<ContextMgr.CollectionMeta> collections) {
        List<String> names = new ArrayList<>();
        for (ContextMgr.CollectionMeta collection : collections) {
            names.add(qualifiedCollectionName(mgr, collection));
        }
        return names.toString();
    }

    private static String configuredWorkspaceRoute(ContextMgr.WorkspaceMeta workspace,
                                                   ContextMgr.CollectionMeta workspaceCollection,
                                                   ContextMgr.ContextBaseMeta contextBase,
                                                   String propertyKey) {
        return firstNonEmpty(
                workspace == null || workspace.getProperties() == null ? null : workspace.getProperties().get(propertyKey),
                workspaceCollection == null || workspaceCollection.getProperties() == null
                        ? null : workspaceCollection.getProperties().get(propertyKey),
                contextBase == null || contextBase.getProperties() == null ? null : contextBase.getProperties().get(propertyKey));
    }

    private static String firstNonEmpty(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!Strings.isNullOrEmpty(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private static String workspaceContextBase(String workspaceName) {
        int firstDot = workspaceName == null ? -1 : workspaceName.indexOf('.');
        return firstDot > 0 ? workspaceName.substring(0, firstDot) : null;
    }

    private List<WorkspaceObjectRow> loadLatestWorkspaceObjects(long workspaceId) {
        // Fold "latest version per object_id" on the BE via ROW_NUMBER instead of pulling every
        // version of every object into FE memory and dedup-ing in Java. The previous approach
        // scaled with total versions; the new shape scales with distinct object_ids.
        String pickSql = String.format(
                "SELECT object_id, version, workspace_scope, payload_json, deleted FROM ("
                        + "SELECT object_id, version, workspace_scope, payload_json, deleted, "
                        + "ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY version DESC) AS rn "
                        + "FROM %s.%s WHERE workspace_id = %d) t WHERE rn = 1 ORDER BY object_id ASC",
                ContextInternalTables.DATABASE, ContextInternalTables.WORKSPACE_OBJECTS, workspaceId);
        JsonArray rows = runRequiredQuery(pickSql);
        List<WorkspaceObjectRow> latest = new ArrayList<>();
        for (JsonElement rowEl : rows) {
            JsonArray data = rowEl.getAsJsonObject().getAsJsonArray("data");
            String objectId = stringColumn(data, 0);
            if (objectId == null) {
                continue;
            }
            latest.add(new WorkspaceObjectRow(
                    objectId,
                    WorkspaceObjectWriter.normalizeWorkspaceScopeForRead(stringColumn(data, 2)),
                    jsonColumnToString(data.size() > 3 ? data.get(3) : null),
                    booleanColumn(data, 4)));
        }
        return latest;
    }

    private static String stringColumn(JsonArray data, int idx) {
        if (data == null || data.size() <= idx || data.get(idx).isJsonNull()) {
            return null;
        }
        return data.get(idx).getAsString();
    }

    private static boolean booleanColumn(JsonArray data, int idx) {
        return data != null && data.size() > idx && ContextJsonUtil.parseBool(data.get(idx));
    }

    private static final class WorkspaceObjectRow {
        private final String objectId;
        private final String workspaceScope;
        private final String payloadJson;
        private final boolean deleted;

        private WorkspaceObjectRow(String objectId, String workspaceScope, String payloadJson, boolean deleted) {
            this.objectId = objectId;
            this.workspaceScope = workspaceScope;
            this.payloadJson = payloadJson;
            this.deleted = deleted;
        }
    }

    private static String numberLiteral(Object o) {
        if (o instanceof Number) {
            return Long.toString(((Number) o).longValue());
        }
        return String.valueOf(o);
    }

    private static String extractStringField(JsonArray rows, int idx) {
        if (rows == null || rows.size() == 0) {
            return null;
        }
        JsonArray data = rows.get(0).getAsJsonObject().getAsJsonArray("data");
        if (data.size() <= idx || data.get(idx).isJsonNull()) {
            return null;
        }
        return data.get(idx).getAsString();
    }

    /**
     * Workspace payloads are arbitrary JSON. If it has a top-level {@code content} field, treat
     * that as the entity body; otherwise fall back to the entire payload as a JSON-serialized
     * blob. This matches the convention REST callers use when writing workspace objects.
     */
    private static String extractContent(String payloadJson) {
        try {
            JsonElement parsed = JsonParser.parseString(payloadJson);
            if (parsed.isJsonObject() && parsed.getAsJsonObject().has("content")) {
                JsonElement content = parsed.getAsJsonObject().get("content");
                if (content.isJsonPrimitive()) {
                    return content.getAsString();
                }
                return content.toString();
            }
        } catch (Exception ignored) {
            // tolerate malformed payloads — fall through and pass them as-is
        }
        return payloadJson;
    }

    /**
     * Compare-and-swap claim: transitions {@code PENDING → RUNNING} only when the row is still
     * pending, and writes a unique lease token into {@code error_message}. Returns {@code true}
     * iff this caller now owns the task. Without this guard, a stale leader and a new leader can
     * both pick the same PENDING row, both run {@link #markRunning(long)} as an unconditional
     * UPDATE, and both invoke the non-idempotent handler — duplicating bulk-import inserts,
     * derived-page generations, etc.
     */
    private boolean claimRunning(long taskId) {
        String now = TS_FMT.format(LocalDateTime.now());
        String lease = FE_INSTANCE_ID + ":" + leaseCounter.incrementAndGet();
        String escapedLease = ContextSqlEscape.body(lease);
        String sql = String.format(
                "UPDATE %s.%s SET state = 'RUNNING', updated_time = '%s', error_message = '%s' "
                        + "WHERE task_id = %d AND state = 'PENDING'",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS, now, escapedLease, taskId);
        try {
            SimpleExecutor.getRepoExecutor().executeDML(sql);
        } catch (Exception e) {
            LOG.warn("{} claimRunning failed for task {}: {}", getName(), taskId, e.getMessage());
            return false;
        }
        // Verify: re-read the lease token. If a concurrent claim won, error_message holds
        // their lease, not ours.
        String verifySql = String.format(
                "SELECT error_message FROM %s.%s WHERE task_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS, taskId);
        JsonArray rows;
        try {
            rows = ContextSqlSupport.executeDql(verifySql);
        } catch (Exception e) {
            LOG.warn("{} claimRunning verify failed for task {}: {}", getName(), taskId, e.getMessage());
            return false;
        }
        if (rows.size() == 0) {
            return false;
        }
        JsonElement v = rows.get(0).getAsJsonObject().getAsJsonArray("data").get(0);
        return !v.isJsonNull() && lease.equals(v.getAsString());
    }

    private void markCompleted(long taskId) {
        String now = TS_FMT.format(LocalDateTime.now());
        // Clear error_message: it was holding our lease token, which is meaningless once the
        // task reaches a terminal state.
        String sql = String.format(
                "UPDATE %s.%s SET state = 'COMPLETED', updated_time = '%s', error_message = NULL "
                        + "WHERE task_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS, now, taskId);
        SimpleExecutor.getRepoExecutor().executeDML(sql);
    }

    private void markFailed(long taskId, String errorMessage) {
        String now = TS_FMT.format(LocalDateTime.now());
        SimpleExecutor.getRepoExecutor().executeDML(buildFailedStateSql(taskId, now, errorMessage));
    }

    static String buildFailedStateSql(long taskId, String updatedTime, String errorMessage) {
        String raw = errorMessage == null ? "" : errorMessage;
        if (raw.isEmpty()) {
            return String.format(
                    "UPDATE %s.%s SET state = 'FAILED', updated_time = '%s', error_message = '' "
                            + "WHERE task_id = %d",
                    ContextInternalTables.DATABASE, ContextInternalTables.TASKS, updatedTime, taskId);
        }
        String trimmed = trimErrorMessageForSql(raw, 1900);
        String escaped = ContextSqlEscape.body(trimmed);
        return String.format(
                "UPDATE %s.%s SET state = 'FAILED', updated_time = '%s', error_message = '%s' "
                        + "WHERE task_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS, updatedTime, escaped, taskId);
    }

    private static String trimErrorMessageForSql(String raw, int maxEscapedLength) {
        if (raw == null || raw.isEmpty()) {
            return "";
        }
        if (ContextSqlEscape.body(raw).length() <= maxEscapedLength) {
            return raw;
        }
        int low = 0;
        int high = raw.length();
        while (low < high) {
            int mid = (low + high + 1) >>> 1;
            String candidate = raw.substring(0, mid);
            if (ContextSqlEscape.body(candidate).length() <= maxEscapedLength) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return raw.substring(0, low);
    }

    /**
     * StarRocks JSON columns can come back from {@code SimpleExecutor} either as a string
     * (when the encoder serialized them as primitives) or as a structured Json* element
     * (when the BE decoded the JSON value first). {@link JsonElement#getAsString()} only
     * handles the primitive shape and throws {@code UnsupportedOperationException} on
     * objects/arrays. This helper papers over the difference: it returns {@code null} for
     * null elements, the primitive value for strings/numbers, and the JSON wire form for
     * structured values.
     */
    private static String jsonColumnToString(JsonElement el) {
        if (el == null || el.isJsonNull()) {
            return null;
        }
        if (el.isJsonPrimitive()) {
            return el.getAsString();
        }
        return el.toString();
    }

    private JsonArray runQuery(String sql) {
        try {
            return ContextSqlSupport.executeDql(sql);
        } catch (Exception e) {
            LOG.debug("{} query failed (tables not ready?): {}", getName(), e.getMessage());
            return new JsonArray();
        }
    }

    private JsonArray runRequiredQuery(String sql) {
        try {
            return ContextSqlSupport.executeDql(sql);
        } catch (Exception e) {
            throw new IllegalStateException("context internal query failed: " + e.getMessage(), e);
        }
    }
}
